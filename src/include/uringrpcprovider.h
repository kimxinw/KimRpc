#pragma once

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <liburing.h>
#include "zookeeperutil.h"

#include"connectionmanager.h"
#include "coroutine/task.h"

#include <coroutine>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <queue>
#include <string>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>
#include <vector>
#include <netinet/tcp.h>


class UringRpcProvider
{
public:
    UringRpcProvider();
    ~UringRpcProvider();

    UringRpcProvider(const UringRpcProvider &) = delete;
    UringRpcProvider &operator=(const UringRpcProvider &) = delete;

    void NotifyService(google::protobuf::Service *service);
    void Run();

private:
    struct ServiceInfo
    {
        google::protobuf::Service *m_service;
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap;
    };

    enum class OpType
    {
        ACCEPT,
        RECV,
        SEND,
        NOTIFY
    };

    // co_await 它：把同步的 CallMethod offload 到线程池执行并挂起协程；业务的 done
    // 回调在 worker 线程把成帧响应写入 *out（指向协程的局部变量），再把协程句柄投回
    // reactor 线程恢复。
    //
    // 注意 await_resume 必须返回 void：GCC 11 的协程 codegen 有 bug —— 若从
    // await_resume 按值返回 std::string 去初始化协程局部，局部串的内部指针会错误地
    // 指向协程帧内存，析构时 bad-free 砸坏堆。因此响应字节经"协程局部 + 指针写入"传递，
    // 不走 await_resume 返回值。
    struct CallOnPoolAwaiter
    {
        UringRpcProvider *self;
        google::protobuf::Service *service;
        const google::protobuf::MethodDescriptor *method;
        google::protobuf::Message *request;
        google::protobuf::Message *response;
        uint64_t request_id;
        std::string *out; // 指向协程局部 frame：worker 线程写、reactor resume 后读

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h);
        void await_resume() noexcept {}
    };

    void startListen(const std::string &ip, uint16_t port);
    void registerServiceToZk(const std::string &ip, uint16_t port);
    bool setupBufRing();
    void eventLoop();

    bool submitAccept();
    bool submitRecv(int fd);
    bool submitSend(int fd);
    bool submitNotifyRead();

    void handleAccept(int result);
    void handleRecv(ConnectionContext* ctx, int result);
    void handleSend(ConnectionContext* ctx, int result);
    void handleNotify();

    bool tryHandleRequest(int fd, ConnectionContext *ctx);

    // 每个请求一个协程：offload CallMethod 到线程池，回到 reactor 线程后把响应入队发送
    kimrpc::coro::DetachedTask handleRpc(int fd, uint64_t connId, uint64_t request_id,
                                         google::protobuf::Service *service,
                                         const google::protobuf::MethodDescriptor *method,
                                         google::protobuf::Message *request);
    // 业务 done 回调（worker 线程）：序列化响应并把协程句柄投回 reactor
    void onPoolDone(CallOnPoolAwaiter *aw, std::coroutine_handle<> h);
    // 跨线程唤醒：把待恢复协程句柄入队并 eventfd 通知 reactor
    void resumeOnReactor(std::coroutine_handle<> h);
    // reactor 线程：connId 校验后把成帧响应推入 outQueue 并启动发送
    void deliverResponse(int fd, uint64_t connId, std::string data);
    // void closeConnection(int fd);

    void submitTask(std::function<void()> task);
    void workerThread();

    // void recycleConn(ConnectionContext* ctx);

    // IO
    int m_listenfd;
    io_uring m_ring;
    bool m_ringReady;
    bool m_acceptPending;
    bool m_notifyPending;

    // provided buffer ring（所有连接共享的内核可选缓冲池）
    io_uring_buf_ring *m_bufRing = nullptr;
    void *m_bufBase = nullptr;

    // Worker → main notification
    int m_notifyFd;
    // 待 reactor 线程恢复的协程句柄（worker 线程投递，handleNotify 取走）
    std::vector<std::coroutine_handle<>> m_readyCoros;
    std::mutex m_readyMutex;

    // Thread pool
    std::vector<std::thread> m_threadPool;
    std::queue<std::function<void()>> m_taskQueue;
    std::mutex m_taskMutex;
    std::condition_variable m_taskCv;
    bool m_poolStop;

    std::unordered_map<std::string, ServiceInfo> m_serviceMap;
    
    ZkClient m_zkClient;

    static __u64 packUserData(ConnectionContext* ctx, OpType op);
    static ConnectionContext* unpackCtx(__u64 ud);
    static OpType unpackOp(__u64 ud);
    ConnectionManager m_connMgr;
    char m_notifyBuf[8];
};
