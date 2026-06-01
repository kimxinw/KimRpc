#pragma once
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <unistd.h>  // ::close
#include <atomic>

struct ConnectionContext
{
    int fd;
    uint64_t connId; // 单调递增的连接代号，跨线程回包时校验 fd 是否已被复用
    std::string input;
    std::deque<std::string> outQueue; // 待发送响应队列（多路复用：同一连接多个响应排队）
    size_t written;                   // 队首响应已发送的字节数
    bool sending;                     // 是否已有一个 send 在途（保证同一连接同时只挂一个 send）
    char buffer[4096];

    int inflight;
    bool closing;

    int pendingReqs;  // 已派发给业务线程、响应尚未发完的请求数（背压计数）
    bool readPaused;  // 因背压暂停了 recv（达到 in-flight 上限），待响应发完后恢复

    void reset(int newfd)
    {
        fd = newfd;
        input.clear();
        outQueue.clear();
        written = 0;
        sending = false;
        inflight = 0;
        closing = false;
        pendingReqs = 0;
        readPaused = false;
    }
};

class ConnectionManager
{
public:
    ConnectionManager() = default;
    ~ConnectionManager() = default;

    ConnectionManager(const ConnectionManager &) = delete;
    ConnectionManager &operator=(const ConnectionManager &) = delete;

    // 新连接：从池取或新建，登记进活跃表
    ConnectionContext *acquire(int fd);

    // 按 fd 查活跃连接，找不到返回 nullptr
    ConnectionContext *find(int fd);

    // 提交一个 io_uring 操作时调用
    void onSubmit(ConnectionContext *ctx) { ++ctx->inflight; }

    // 一个 cqe 回来时调用，返回 true 表示该 ctx 已被回收、调用方不应再使用它
    bool onComplete(ConnectionContext *ctx);

    // 请求关闭连接（关 fd、移出活跃表、标记 closing）
    // 若无在途操作则立即回收，否则等最后一个 cqe
    void close(int fd);
    

private:
    void recycle(ConnectionContext *ctx);

    std::unordered_map<int, ConnectionContext *> m_active;       // fd -> 活跃连接
    std::vector<std::unique_ptr<ConnectionContext>> m_allConns;  // 所有权
    std::vector<ConnectionContext *> m_idlePool;                 // 空闲池
    size_t m_idlePoolSize = 100;
    std::atomic<uint64_t> nextId{1};  // 0 留作无效 connId 哨兵
};