#pragma once

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <liburing.h>
#include "zookeeperutil.h"

#include"connectionmanager.h"

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

    
    
    struct PendingResponse
    {
        int fd;
        uint64_t connId;
        std::string data; // size-prefixed wire bytes
    };

    // 随 done 回调传递给业务线程，回包时用来定位连接并回填 request_id
    struct ResponseTag
    {
        int fd;
        uint64_t request_id;
        uint64_t connId;
    };

    void startListen(const std::string &ip, uint16_t port);
    void registerServiceToZk(const std::string &ip, uint16_t port);
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
    void sendRpcResponse(ResponseTag *tag, google::protobuf::Message *response);
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

    // Worker → main notification
    int m_notifyFd;
    std::vector<PendingResponse> m_pendingResponses;
    std::mutex m_pendingMutex;

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
