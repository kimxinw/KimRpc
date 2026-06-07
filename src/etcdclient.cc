#include "etcdclient.h"

#include "application.h"
#include "logger.h"
#include "third_party/json.hpp"

#include <curl/curl.h>

#include <chrono>
#include <cstdint>

using json = nlohmann::json;

namespace
{
// 进程级 curl 初始化（线程安全，只做一次）
std::once_flag g_curlInitFlag;
void ensureCurlInit()
{
    std::call_once(g_curlInitFlag, [] { curl_global_init(CURL_GLOBAL_DEFAULT); });
}

// ── base64（标准字母表，带 padding）：etcd v3 JSON 里 key/value 均为 base64 ──
const char kB64Tab[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string b64Encode(const std::string &in)
{
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    size_t i = 0;
    while (i + 2 < in.size())
    {
        unsigned n = (static_cast<unsigned char>(in[i]) << 16) |
                     (static_cast<unsigned char>(in[i + 1]) << 8) |
                     (static_cast<unsigned char>(in[i + 2]));
        out.push_back(kB64Tab[(n >> 18) & 0x3f]);
        out.push_back(kB64Tab[(n >> 12) & 0x3f]);
        out.push_back(kB64Tab[(n >> 6) & 0x3f]);
        out.push_back(kB64Tab[n & 0x3f]);
        i += 3;
    }
    if (i < in.size())
    {
        unsigned n = static_cast<unsigned char>(in[i]) << 16;
        bool two = (i + 1 < in.size());
        if (two)
            n |= static_cast<unsigned char>(in[i + 1]) << 8;
        out.push_back(kB64Tab[(n >> 18) & 0x3f]);
        out.push_back(kB64Tab[(n >> 12) & 0x3f]);
        out.push_back(two ? kB64Tab[(n >> 6) & 0x3f] : '=');
        out.push_back('=');
    }
    return out;
}

int b64Val(char c)
{
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

std::string b64Decode(const std::string &in)
{
    std::string out;
    int val = 0, bits = -8;
    for (char c : in)
    {
        if (c == '=' || c == '\n' || c == '\r')
            continue;
        int d = b64Val(c);
        if (d < 0)
            continue;
        val = (val << 6) | d;
        bits += 6;
        if (bits >= 0)
        {
            out.push_back(static_cast<char>((val >> bits) & 0xff));
            bits -= 8;
        }
    }
    return out;
}

// etcd 把 int64 字段（ID/revision/TTL）编码成 JSON 字符串，这里统一取
int64_t jint(const json &j, const char *field, int64_t def = 0)
{
    auto it = j.find(field);
    if (it == j.end())
        return def;
    if (it->is_string())
    {
        try { return std::stoll(it->get<std::string>()); } catch (...) { return def; }
    }
    if (it->is_number())
        return it->get<int64_t>();
    return def;
}

// 计算前缀扫描的 range_end：最后一个 < 0xff 的字节 +1 并截断
std::string prefixEnd(std::string p)
{
    for (int i = static_cast<int>(p.size()) - 1; i >= 0; --i)
    {
        if (static_cast<unsigned char>(p[i]) < 0xff)
        {
            p[i] = static_cast<char>(static_cast<unsigned char>(p[i]) + 1);
            p.resize(i + 1);
            return p;
        }
    }
    return std::string("\0", 1); // 全 0xff：扫描到末尾
}

size_t writeToString(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    static_cast<std::string *>(userdata)->append(ptr, size * nmemb);
    return size * nmemb;
}

// ── watch 流式解析上下文 ──
struct WatchCtx
{
    std::string buf;                    // 跨 chunk 的行缓冲
    EtcdClient::WatchCallback *cb;
    std::atomic<bool> *stop;
    int64_t lastRevision; // 已处理到的最高 revision（仅由“带 events 的帧”推进）
};

void parseWatchLine(const std::string &line, WatchCtx *ctx, const std::string &prefix)
{
    json root;
    try { root = json::parse(line); } catch (...) { return; }
    auto rit = root.find("result");
    if (rit == root.end())
        return; // 可能是 error 帧，忽略
    const json &result = *rit;

    auto eit = result.find("events");
    if (eit == result.end() || !eit->is_array())
        return; // created/无事件帧：不推进 revision

    for (const auto &ev : *eit)
    {
        EtcdClient::WatchEvent we;
        // type 缺省（protojson 省略 0 值）即 PUT
        std::string type = ev.value("type", std::string());
        we.type = (type == "DELETE") ? EtcdClient::EventType::Delete : EtcdClient::EventType::Put;
        auto kit = ev.find("kv");
        if (kit != ev.end())
        {
            we.key = b64Decode(kit->value("key", std::string()));
            we.value = b64Decode(kit->value("value", std::string()));
        }
        (*ctx->cb)(we);
    }

    // 整帧事件已全部回调，推进到该 revision，断线后从 +1 续上不丢不重读未处理部分
    auto hit = result.find("header");
    if (hit != result.end())
    {
        int64_t rev = jint(*hit, "revision", 0);
        if (rev > ctx->lastRevision)
            ctx->lastRevision = rev;
    }
    (void)prefix;
}

size_t watchWrite(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *ctx = static_cast<WatchCtx *>(userdata);
    if (ctx->stop->load(std::memory_order_relaxed))
        return 0; // 返回与入参不等 → 中止传输
    size_t n = size * nmemb;
    ctx->buf.append(ptr, n);
    size_t pos;
    while ((pos = ctx->buf.find('\n')) != std::string::npos)
    {
        std::string line = ctx->buf.substr(0, pos);
        ctx->buf.erase(0, pos + 1);
        if (!line.empty())
            parseWatchLine(line, ctx, std::string());
    }
    return n;
}

// 进度回调：用于在没有数据到达时也能感知 stop，及时中止 watch 长连接
int watchProgress(void *clientp, curl_off_t, curl_off_t, curl_off_t, curl_off_t)
{
    auto *stop = static_cast<std::atomic<bool> *>(clientp);
    return stop->load(std::memory_order_relaxed) ? 1 : 0;
}
} // namespace

EtcdClient::~EtcdClient()
{
    Stop();
}

bool EtcdClient::Start()
{
    std::string ip = KimRpcApplication::GetInstance().GetConfig().load("etcdip");
    std::string port = KimRpcApplication::GetInstance().GetConfig().load("etcdport");
    if (ip.empty())
        ip = "127.0.0.1";
    if (port.empty())
        port = "2379";
    return Start(ip, port);
}

bool EtcdClient::Start(const std::string &ip, const std::string &port)
{
    ensureCurlInit();
    m_endpoint = "http://" + ip + ":" + port;

    // 探活：申请一个很短的探测租约，成功即认为连通（探测租约会很快自然过期）
    int64_t probe = LeaseGrant(2);
    if (probe <= 0)
    {
        LOG_ERROR("etcd connect failed at %s", m_endpoint.c_str());
        return false;
    }
    return true;
}

std::string EtcdClient::httpPost(const std::string &path, const std::string &body, long timeoutMs)
{
    ensureCurlInit();
    CURL *curl = curl_easy_init();
    if (curl == nullptr)
        return std::string();

    std::string url = m_endpoint + path;
    std::string resp;
    curl_slist *headers = curl_slist_append(nullptr, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToString);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 2000L);
    if (timeoutMs > 0)
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeoutMs);

    CURLcode rc = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK)
        return std::string();
    return resp;
}

int64_t EtcdClient::LeaseGrant(int ttlSec)
{
    std::string body = "{\"TTL\":" + std::to_string(ttlSec) + ",\"ID\":0}";
    std::string resp = httpPost("/v3/lease/grant", body);
    if (resp.empty())
        return -1;
    try
    {
        json j = json::parse(resp);
        return jint(j, "ID", -1);
    }
    catch (...)
    {
        return -1;
    }
}

bool EtcdClient::Put(const std::string &key, const std::string &value, int64_t leaseId)
{
    std::string body = "{\"key\":\"" + b64Encode(key) + "\",\"value\":\"" + b64Encode(value) + "\"";
    if (leaseId > 0)
        body += ",\"lease\":" + std::to_string(leaseId);
    body += "}";
    std::string resp = httpPost("/v3/kv/put", body);
    if (resp.empty())
        return false;
    // 成功响应含 header；解析失败或含 error 视为失败
    try
    {
        json j = json::parse(resp);
        return j.find("error") == j.end() && j.find("header") != j.end();
    }
    catch (...)
    {
        return false;
    }
}

bool EtcdClient::RangePrefix(const std::string &prefix, std::vector<KeyValue> *out, int64_t *revision)
{
    std::string body = "{\"key\":\"" + b64Encode(prefix) + "\",\"range_end\":\"" +
                       b64Encode(prefixEnd(prefix)) + "\"}";
    std::string resp = httpPost("/v3/kv/range", body);
    if (resp.empty())
        return false;
    try
    {
        json j = json::parse(resp);
        if (j.find("error") != j.end())
            return false;
        auto hit = j.find("header");
        if (revision != nullptr && hit != j.end())
            *revision = jint(*hit, "revision", 0);
        if (out != nullptr)
        {
            out->clear();
            auto kit = j.find("kvs");
            if (kit != j.end() && kit->is_array())
            {
                for (const auto &kv : *kit)
                {
                    KeyValue e;
                    e.key = b64Decode(kv.value("key", std::string()));
                    e.value = b64Decode(kv.value("value", std::string()));
                    out->push_back(std::move(e));
                }
            }
        }
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void EtcdClient::StartKeepAlive(int64_t leaseId, int ttlSec)
{
    if (leaseId <= 0)
        return;
    m_keepAliveThread = std::thread(&EtcdClient::keepAliveLoop, this, leaseId, ttlSec);
}

void EtcdClient::keepAliveLoop(int64_t leaseId, int ttlSec)
{
    int interval = ttlSec / 3;
    if (interval < 1)
        interval = 1;
    std::string body = "{\"ID\":" + std::to_string(leaseId) + "}";

    while (true)
    {
        {
            std::unique_lock<std::mutex> lk(m_cvMtx);
            if (m_cv.wait_for(lk, std::chrono::seconds(interval),
                              [this] { return m_stop.load(); }))
            {
                return; // 收到停止
            }
        }
        std::string resp = httpPost("/v3/lease/keepalive", body);
        if (resp.empty())
        {
            LOG_ERROR("etcd lease keepalive request failed (lease=%ld)", static_cast<long>(leaseId));
            continue; // 网络抖动：下个周期重试
        }
        // 响应：{"result":{"ID":"..","TTL":".."}}，TTL=0 表示租约已失效，尝试重建
        try
        {
            json j = json::parse(resp);
            auto rit = j.find("result");
            const json &r = (rit != j.end()) ? *rit : j;
            if (jint(r, "TTL", -1) == 0)
            {
                LOG_ERROR("etcd lease %ld expired (keepalive TTL=0)", static_cast<long>(leaseId));
            }
        }
        catch (...)
        {
        }
    }
}

void EtcdClient::WatchPrefixBlocking(const std::string &prefix, int64_t startRev,
                                     WatchCallback cb, std::atomic<bool> &stop)
{
    ensureCurlInit();
    int64_t resumeRev = startRev;

    while (!stop.load(std::memory_order_relaxed))
    {
        CURL *curl = curl_easy_init();
        if (curl == nullptr)
            return;

        // create_request：监听 [prefix, prefixEnd) 区间，从 resumeRev 开始
        std::string body = "{\"create_request\":{\"key\":\"" + b64Encode(prefix) +
                           "\",\"range_end\":\"" + b64Encode(prefixEnd(prefix)) +
                           "\",\"start_revision\":" + std::to_string(resumeRev) + "}}";

        WatchCtx ctx;
        ctx.cb = &cb;
        ctx.stop = &stop;
        ctx.lastRevision = resumeRev - 1;

        std::string url = m_endpoint + "/v3/watch";
        curl_slist *headers = curl_slist_append(nullptr, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, watchWrite);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 2000L);
        // 长连接：不设整体超时，靠进度回调感知 stop 中止
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, watchProgress);
        curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &stop);

        curl_easy_perform(curl); // 阻塞直到流断开 / stop 中止
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        // 断线续连：从已处理到的 revision+1 接着监听
        if (ctx.lastRevision >= resumeRev)
            resumeRev = ctx.lastRevision + 1;

        if (stop.load(std::memory_order_relaxed))
            break;
        // 避免 etcd 不可达时忙循环重连
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

void EtcdClient::Stop()
{
    {
        std::lock_guard<std::mutex> lk(m_cvMtx);
        m_stop.store(true);
    }
    m_cv.notify_all();
    if (m_keepAliveThread.joinable())
        m_keepAliveThread.join();
}
