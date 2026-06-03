#pragma once

// 非阻塞客户端开关：定义宏让 KimRpcChannel::CallMethod 变为非阻塞（依赖 done 回调，
// 传 nullptr 时框架内部退化为同步）。取消则为传统阻塞实现。
// #define KIMRPC_ASYNC_CLIENT

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

class KimRpcChannel : public google::protobuf::RpcChannel
{
public:
    KimRpcChannel();
    ~KimRpcChannel();

    KimRpcChannel(const KimRpcChannel &) = delete;
    KimRpcChannel &operator=(const KimRpcChannel &) = delete;

    // 所有通过stub代理对象调用的rpc方法都走到这里了，统一做rpc方法调用的数据序列化和网络发送
    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;

private:
    // 等待中的一次调用，按编译模式有两种唤醒方式
    struct Pending
    {
#ifdef KIMRPC_ASYNC_CLIENT
        // 非阻塞模式：收线程把响应直接解析进 response，再触发 done 回调
        google::protobuf::Message *response;
        google::protobuf::RpcController *controller; // 可能为空
        google::protobuf::Closure *done;             // 完成回调，非空
#else
        // 阻塞模式：响应体写入 response_buf，通过 prom 唤醒调用线程
        std::string *response_buf;
        std::promise<bool> prom;
#endif
    };

#ifdef KIMRPC_ASYNC_CLIENT
    // 非阻塞内核：注册等待槽后立即返回，响应到达时由收线程触发 done。
    // 失败（地址/连接/序列化/发送）时也保证恰好触发一次 done。
    void callAsync(const google::protobuf::MethodDescriptor *method,
                   google::protobuf::RpcController *controller,
                   const google::protobuf::Message *request,
                   google::protobuf::Message *response,
                   google::protobuf::Closure *done);
#endif

    bool ensureConnected(const std::string &host_data, std::string *error);
    bool sendLocked(const char *data, size_t len, std::string *error);
    bool recvAll(char *data, size_t len); // 仅供收线程使用，失败返回 false 不关闭 fd
    void recvLoop();
    void teardown();                  // 关闭连接、join 收线程、唤醒所有等待者
    void failAllPending(const std::string &reason);

    // 连接状态：由 m_connMtx 保护
    std::mutex m_connMtx;
    int m_clientfd;
    std::string m_connectedHost;
    std::thread m_recvThread;
    std::atomic<bool> m_running;

    std::mutex m_sendMtx; // 串行化 socket 写，避免多线程交错写帧

    // request_id -> 等待槽：由 m_pendingMtx 保护
    std::mutex m_pendingMtx;
    std::unordered_map<uint64_t, Pending *> m_pending;
    std::atomic<uint64_t> m_nextId;

};
