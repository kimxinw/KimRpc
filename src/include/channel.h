#pragma once

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
    // 等待中的一次调用：响应体写入 response_buf，通过 prom 唤醒调用线程
    struct Pending
    {
        std::string *response_buf;
        std::promise<bool> prom;
    };

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
