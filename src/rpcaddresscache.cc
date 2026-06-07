#include "rpcaddresscache.h"

#include "etcdclient.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// 服务发现 + 客户端负载均衡（etcd 版）
//
//   首次请求某 method：同步 RangePrefix 拉全量实例 + revision，起 watch 线程从
//   revision+1 持续消费 PUT/DELETE 增量；之后每次调用按轮询(round-robin)返回一个实例。
//   GetRpcAddress 维持原型，channel/uringchannel 零改动。
//
// 实例身份取 key 后缀：key = "<method_path>/<ip:port>"，prefix = "<method_path>/"，
// 故 addr = key.substr(prefix.size()) = "ip:port"（PUT/DELETE 统一，DELETE 无 value 也适用）。

namespace
{
struct MethodEntry
{
    std::mutex mtx;
    std::vector<std::string> instances; // "ip:port" 列表
    std::atomic<size_t> rr{0};          // round-robin 游标
    std::atomic<bool> stop{false};      // 客户端进程生命周期内不置位（watch 随进程退出）
};

// g_etcd 与各 MethodEntry 都用 new 分配且永不释放（进程级泄漏，刻意为之）：
// watch 线程被 detach 后存活到进程退出，若这些对象在静态析构期被销毁，detached
// 线程仍会访问 → 释放后使用。泄漏让内存始终有效，退出时由 OS 统一回收。
std::mutex g_mtx;
std::unordered_map<std::string, MethodEntry *> g_entries;
EtcdClient *g_etcd = nullptr;
std::once_flag g_etcdInit;
bool g_etcdOk = false;

void ensureEtcd()
{
    std::call_once(g_etcdInit, [] {
        g_etcd = new EtcdClient();
        g_etcdOk = g_etcd->Start();
    });
}
} // namespace

bool GetRpcAddress(const std::string &method_path, std::string *host_data, std::string *error)
{
    ensureEtcd();
    if (!g_etcdOk)
    {
        if (error != nullptr)
            *error = "etcd not reachable";
        return false;
    }

    MethodEntry *entry = nullptr;
    {
        std::lock_guard<std::mutex> lk(g_mtx);
        auto it = g_entries.find(method_path);
        if (it != g_entries.end())
        {
            entry = it->second;
        }
        else
        {
            const std::string prefix = method_path + "/";
            std::vector<EtcdClient::KeyValue> kvs;
            int64_t rev = 0;
            if (!g_etcd->RangePrefix(prefix, &kvs, &rev))
            {
                if (error != nullptr)
                    *error = method_path + " etcd range failed";
                return false;
            }

            entry = new MethodEntry();
            for (auto &kv : kvs)
            {
                if (kv.key.size() > prefix.size())
                    entry->instances.push_back(kv.key.substr(prefix.size()));
            }

            // watch 线程：从 rev+1 起增量维护实例表（detach，随进程退出）
            std::thread([entry, prefix, rev] {
                g_etcd->WatchPrefixBlocking(
                    prefix, rev + 1,
                    [entry, prefix](const EtcdClient::WatchEvent &ev) {
                        if (ev.key.size() <= prefix.size())
                            return;
                        std::string addr = ev.key.substr(prefix.size());
                        std::lock_guard<std::mutex> lk(entry->mtx);
                        auto &vec = entry->instances;
                        if (ev.type == EtcdClient::EventType::Put)
                        {
                            if (std::find(vec.begin(), vec.end(), addr) == vec.end())
                                vec.push_back(addr);
                        }
                        else
                        {
                            vec.erase(std::remove(vec.begin(), vec.end(), addr), vec.end());
                        }
                    },
                    entry->stop);
            }).detach();

            g_entries[method_path] = entry;
        }
    }

    std::lock_guard<std::mutex> lk(entry->mtx);
    if (entry->instances.empty())
    {
        if (error != nullptr)
            *error = method_path + " has no available instance";
        return false;
    }
    size_t idx = entry->rr.fetch_add(1, std::memory_order_relaxed) % entry->instances.size();
    *host_data = entry->instances[idx];
    return true;
}
