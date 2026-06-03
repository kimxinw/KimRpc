#include "channel.h"

#include <google/protobuf/stubs/callback.h>

#include "application.h"
#include "controller.h"
#include "rpcaddresscache.h"
#include "rpcheader.pb.h"

#include <errno.h>
#include <arpa/inet.h>
#include <cstdio>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>

KimRpcChannel::KimRpcChannel()
    : m_clientfd(-1), m_running(false), m_nextId(1)
{
}

KimRpcChannel::~KimRpcChannel()
{
    teardown();
}

// 关闭连接、唤醒收线程并 join、失败掉所有还在等待的调用
void KimRpcChannel::teardown()
{
    std::thread t;
    {
        std::lock_guard<std::mutex> lock(m_connMtx);
        m_running = false;
        if (m_clientfd != -1)
        {
            // shutdown 唤醒阻塞在 recv 上的收线程
            ::shutdown(m_clientfd, SHUT_RDWR);
            ::close(m_clientfd);
            m_clientfd = -1;
        }
        m_connectedHost.clear();
        t = std::move(m_recvThread);
    }
    if (t.joinable())
    {
        t.join();
    }
    failAllPending("channel closed");
}

void KimRpcChannel::failAllPending(const std::string &reason)
{
    // 先把等待表整体摘出，再在锁外触发，避免回调里重入 m_pendingMtx 死锁
    std::unordered_map<uint64_t, Pending *> pending;
    {
        std::lock_guard<std::mutex> lock(m_pendingMtx);
        pending.swap(m_pending);
    }
    for (auto &kv : pending)
    {
#ifdef KIMRPC_ASYNC_CLIENT
        if (kv.second->controller != nullptr)
        {
            kv.second->controller->SetFailed(reason);
        }
        kv.second->done->Run();
        delete kv.second;
#else
        kv.second->prom.set_value(false);
#endif
    }
}

// 收线程专用：只读，失败返回 false，不主动关 fd（关闭由 teardown 统一处理）
bool KimRpcChannel::recvAll(char *data, size_t len)
{
    size_t received = 0;
    while (received < len)
    {
        ssize_t ret = recv(m_clientfd, data + received, len - received, 0);
        if (ret <= 0)
        {
            return false;
        }
        received += ret;
    }
    return true;
}

// 后台收线程：持续读响应帧 [4字节 size][8字节 request_id][body]，按 id 派发
void KimRpcChannel::recvLoop()
{
    while (m_running.load())
    {
        uint32_t size = 0;
        if (!recvAll(reinterpret_cast<char *>(&size), sizeof(uint32_t)))
        {
            break;
        }
        if (size < sizeof(uint64_t))
        {
            break; // 帧不合法
        }

        uint64_t id = 0;
        if (!recvAll(reinterpret_cast<char *>(&id), sizeof(uint64_t)))
        {
            break;
        }

        size_t body_len = size - sizeof(uint64_t);
        std::string body;
        body.resize(body_len);
        if (body_len > 0 && !recvAll(&body[0], body_len))
        {
            break;
        }

        Pending *pend = nullptr;
        {
            std::lock_guard<std::mutex> lock(m_pendingMtx);
            auto it = m_pending.find(id);
            if (it != m_pending.end())
            {
                pend = it->second;
                m_pending.erase(it);
            }
            // 未知 id（例如已超时移除）直接丢弃
        }
        if (pend == nullptr)
        {
            continue;
        }
#ifdef KIMRPC_ASYNC_CLIENT
        // 非阻塞：在收线程里直接解析响应并触发回调，调用线程早已返回
        if (!pend->response->ParseFromArray(body.data(), body.size()) && pend->controller != nullptr)
        {
            pend->controller->SetFailed("parse response error!");
        }
        pend->done->Run();
        delete pend;
#else
        *pend->response_buf = std::move(body);
        pend->prom.set_value(true);
#endif
    }

    m_running = false;
    failAllPending("connection closed");
}

bool KimRpcChannel::ensureConnected(const std::string &host_data, std::string *error)
{
    std::lock_guard<std::mutex> lock(m_connMtx);
    if (m_clientfd != -1 && m_running.load() && m_connectedHost == host_data)
    {
        return true;
    }

    // 旧连接已失效：清理并准备重连
    if (m_clientfd != -1)
    {
        ::shutdown(m_clientfd, SHUT_RDWR);
        ::close(m_clientfd);
        m_clientfd = -1;
    }
    if (m_recvThread.joinable())
    {
        m_recvThread.join();
    }
    m_connectedHost.clear();

    int idx = host_data.find(":");
    if (idx == -1)
    {
        *error = host_data + " address is invalid";
        return false;
    }
    std::string ip = host_data.substr(0, idx);
    uint16_t port = stoi(host_data.substr(idx + 1, host_data.size() - idx));

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1)
    {
        char errtxt[512] = {0};
        snprintf(errtxt, sizeof(errtxt), "create socket error! errno:%d", errno);
        *error = errtxt;
        return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());

    if (-1 == connect(fd, (struct sockaddr *)&server_addr, sizeof(server_addr)))
    {
        char errtxt[512] = {0};
        snprintf(errtxt, sizeof(errtxt), "connect error! errno:%d", errno);
        *error = errtxt;
        ::close(fd);
        return false;
    }

    m_clientfd = fd;
    m_connectedHost = host_data;
    m_running = true;
    m_recvThread = std::thread(&KimRpcChannel::recvLoop, this);
    return true;
}

bool KimRpcChannel::sendLocked(const char *data, size_t len, std::string *error)
{
    std::lock_guard<std::mutex> lock(m_sendMtx);
    size_t written = 0;
    while (written < len)
    {
        ssize_t ret = send(m_clientfd, data + written, len - written, 0);
        if (ret <= 0)
        {
            char errtxt[512] = {0};
            snprintf(errtxt, sizeof(errtxt), "send error! errno:%d", errno);
            *error = errtxt;
            return false;
        }
        written += ret;
    }
    return true;
}

/**
 * 请求帧:  header_size(4) + RpcHeader(含 request_id) + args
 * 响应帧:  size(4) + request_id(8) + body
 */
void KimRpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                              google::protobuf::RpcController *controller,
                              const google::protobuf::Message *request,
                              google::protobuf::Message *response,
                              google::protobuf::Closure *done)
{
#ifdef KIMRPC_ASYNC_CLIENT
    // 有回调：纯异步，立即返回。无回调：在异步内核上合成一次同步调用
    if (done != nullptr)
    {
        callAsync(method, controller, request, response, done);
        return;
    }
    struct Synchronizer
    {
        std::promise<void> prom;
        void Notify() { prom.set_value(); }
    } sync;
    std::future<void> fut = sync.prom.get_future();
    google::protobuf::Closure *sync_done =
        google::protobuf::NewCallback(&sync, &Synchronizer::Notify);
    callAsync(method, controller, request, response, sync_done);
    fut.wait(); // 阻塞直到收线程（或内联失败路径）触发回调
    return;
#else
    const google::protobuf::ServiceDescriptor *sd = method->service();
    std::string service_name = sd->name();
    std::string method_name = method->name();

    // 序列化参数 args
    uint32_t args_size = 0;
    std::string args_str;
    if (request->SerializeToString(&args_str))
    {
        args_size = args_str.size();
    }
    else
    {
        controller->SetFailed("serialize request error!");
        return;
    }

    // 为本次调用分配唯一 request_id
    uint64_t request_id = m_nextId.fetch_add(1, std::memory_order_relaxed);

    KimRpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);
    rpcHeader.set_request_id(request_id);

    uint32_t header_size = 0;
    std::string rpc_header_str;
    if (rpcHeader.SerializeToString(&rpc_header_str))
    {
        header_size = rpc_header_str.size();
    }
    else
    {
        controller->SetFailed("serialize rpcHeader error!");
        return;
    }

    std::string send_rpc_str;
    send_rpc_str.insert(0, std::string((char *)&header_size, 4));
    send_rpc_str += rpc_header_str;
    send_rpc_str += args_str;

    std::string method_path = "/" + service_name + "/" + method_name;
    std::string host_data;
    std::string error;
    if (!GetRpcAddress(method_path, &host_data, &error))
    {
        controller->SetFailed(error);
        return;
    }

    if (!ensureConnected(host_data, &error))
    {
        controller->SetFailed(error);
        return;
    }

    // 登记等待槽：Pending 在本函数栈上，收线程通过指针填充，调用方等到 future 后才返回
    std::string body;
    Pending pend;
    pend.response_buf = &body;
    std::future<bool> fut = pend.prom.get_future();
    {
        std::lock_guard<std::mutex> lock(m_pendingMtx);
        m_pending[request_id] = &pend;
    }

    // 发送请求
    if (!sendLocked(send_rpc_str.c_str(), send_rpc_str.size(), &error))
    {
        // 发送失败：撤销等待槽（若收线程已抢先填充则不会找到）
        bool already_set = false;
        {
            std::lock_guard<std::mutex> lock(m_pendingMtx);
            auto it = m_pending.find(request_id);
            if (it != m_pending.end())
            {
                m_pending.erase(it);
            }
            else
            {
                already_set = true;
            }
        }
        if (already_set)
        {
            fut.get(); // 收线程已设置，取走结果避免析构 promise 时悬挂
        }
        controller->SetFailed(error);
        return;
    }

    // 阻塞等待“本次 id”的响应（收线程乱序回来时只唤醒对应调用）
    if (!fut.get())
    {
        controller->SetFailed("connection closed before response");
        return;
    }

    if (!response->ParseFromArray(body.data(), body.size()))
    {
        char errtxt[2048] = {0};
        snprintf(errtxt, sizeof(errtxt), "parse error! response_str:%s", body.c_str());
        controller->SetFailed(errtxt);
        return;
    }

    if (done != nullptr)
    {
        done->Run();
    }
#endif // KIMRPC_ASYNC_CLIENT
}

#ifdef KIMRPC_ASYNC_CLIENT
// 非阻塞内核：注册等待槽后立即返回。请求已在此处同步序列化，故 request 可在返回后释放；
// response/controller/done 必须由调用方保活至回调触发。回调在收线程执行。
void KimRpcChannel::callAsync(const google::protobuf::MethodDescriptor *method,
                             google::protobuf::RpcController *controller,
                             const google::protobuf::Message *request,
                             google::protobuf::Message *response,
                             google::protobuf::Closure *done)
{
    // 任一前置失败都要：写 controller（若有）+ 触发一次 done，保证 caller 不会永久挂起
    auto fail = [&](const std::string &reason) {
        if (controller != nullptr)
        {
            controller->SetFailed(reason);
        }
        done->Run();
    };

    const google::protobuf::ServiceDescriptor *sd = method->service();
    std::string service_name = sd->name();
    std::string method_name = method->name();

    std::string args_str;
    if (!request->SerializeToString(&args_str))
    {
        fail("serialize request error!");
        return;
    }
    uint32_t args_size = args_str.size();

    uint64_t request_id = m_nextId.fetch_add(1, std::memory_order_relaxed);

    KimRpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);
    rpcHeader.set_request_id(request_id);

    std::string rpc_header_str;
    if (!rpcHeader.SerializeToString(&rpc_header_str))
    {
        fail("serialize rpcHeader error!");
        return;
    }
    uint32_t header_size = rpc_header_str.size();

    std::string send_rpc_str;
    send_rpc_str.reserve(sizeof(uint32_t) + header_size + args_size);
    send_rpc_str.append(reinterpret_cast<char *>(&header_size), sizeof(uint32_t));
    send_rpc_str += rpc_header_str;
    send_rpc_str += args_str;

    std::string method_path = "/" + service_name + "/" + method_name;
    std::string host_data;
    std::string error;
    if (!GetRpcAddress(method_path, &host_data, &error))
    {
        fail(error);
        return;
    }
    if (!ensureConnected(host_data, &error))
    {
        fail(error);
        return;
    }

    // 注册等待槽（堆分配，所有权随响应转移给收线程）。必须先注册再发送，
    // 否则响应可能先于登记到达而被当成未知 id 丢弃
    Pending *pend = new Pending{response, controller, done};
    {
        std::lock_guard<std::mutex> lock(m_pendingMtx);
        m_pending[request_id] = pend;
    }

    if (!sendLocked(send_rpc_str.c_str(), send_rpc_str.size(), &error))
    {
        // 发送失败：尝试撤销等待槽。若收线程已抢先取走则由它负责触发 done
        bool taken = false;
        {
            std::lock_guard<std::mutex> lock(m_pendingMtx);
            auto it = m_pending.find(request_id);
            if (it != m_pending.end())
            {
                m_pending.erase(it);
            }
            else
            {
                taken = true;
            }
        }
        if (!taken)
        {
            delete pend;
            fail(error);
        }
        return;
    }
    // 发送成功：响应到达时由收线程解析并触发 done，本函数立即返回
}
#endif // KIMRPC_ASYNC_CLIENT
