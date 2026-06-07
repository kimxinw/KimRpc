#pragma once

// EtcdClient —— 基于 etcd v3 HTTP/JSON 网关的轻量客户端（走 libcurl）
//
// 用途：替换原 ZkClient 做服务注册与发现。
//   provider 侧：Start -> LeaseGrant(ttl) -> Put(每个 method 一个实例 key, 绑 lease)
//               -> StartKeepAlive(lease, ttl)，进程存活则 key 存活，挂了 lease 过期键消失。
//   client 侧 ：Start -> RangePrefix(prefix) 拉全量实例 + revision
//               -> WatchPrefixBlocking(prefix, revision+1, cb, stop) 持续增量更新。
//
// 设计要点：
//   - 每次 HTTP 请求用独立的 curl easy handle（httpPost 内部创建/释放），故各方法
//     可被多线程并发调用而无需加锁（无共享可变状态，m_endpoint 启动后只读）。
//   - keepalive 是 EtcdClient 自管的后台线程（一个 client 一个 lease，贴合 provider）。
//   - watch 是阻塞调用，由调用方（discovery）在自己的线程里跑，用调用方的 stop 标志控制。

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class EtcdClient
{
public:
    EtcdClient() = default;
    ~EtcdClient();

    EtcdClient(const EtcdClient &) = delete;
    EtcdClient &operator=(const EtcdClient &) = delete;

    // 读取配置 etcdip/etcdport 并组装 endpoint；探活一次（lease grant 失败视为连不通）
    bool Start();
    // 用显式地址启动
    bool Start(const std::string &ip, const std::string &port);

    // 申请租约，返回 leaseId（<=0 表示失败）
    int64_t LeaseGrant(int ttlSec);
    // 写入 kv 并绑定 lease（leaseId<=0 表示不绑定，即永久键）
    bool Put(const std::string &key, const std::string &value, int64_t leaseId);

    struct KeyValue
    {
        std::string key;
        std::string value;
    };
    // 按前缀拉全量；out 填充 kv，revision 填充当前存储 revision（用于衔接 watch）
    bool RangePrefix(const std::string &prefix, std::vector<KeyValue> *out, int64_t *revision);

    // 启动后台 keepalive 线程，按 ttlSec/3（>=1s）周期续租
    void StartKeepAlive(int64_t leaseId, int ttlSec);

    enum class EventType
    {
        Put,
        Delete
    };
    struct WatchEvent
    {
        EventType type;
        std::string key;   // 完整 key（已 base64 解码）
        std::string value; // PUT 时为值，DELETE 时通常为空
    };
    using WatchCallback = std::function<void(const WatchEvent &)>;

    // 阻塞式 watch：从 startRev 起监听 prefix，每个事件回调 cb；
    // 流断开自动重连（从已处理到的 revision+1 续上）；stop 置位后尽快返回。
    void WatchPrefixBlocking(const std::string &prefix, int64_t startRev,
                             WatchCallback cb, std::atomic<bool> &stop);

    // 停止并 join keepalive 线程（析构自动调用）
    void Stop();

private:
    // 发一次 POST，返回响应体；失败返回空串。timeoutMs<=0 表示不设整体超时（用于 watch 长连接）
    std::string httpPost(const std::string &path, const std::string &body, long timeoutMs = 3000);
    void keepAliveLoop(int64_t leaseId, int ttlSec);

    std::string m_endpoint; // http://ip:port

    std::atomic<bool> m_stop{false};
    std::thread m_keepAliveThread;
    std::mutex m_cvMtx;
    std::condition_variable m_cv;
};
