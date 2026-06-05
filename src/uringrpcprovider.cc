#include "uringrpcprovider.h"

#include "logger.h"
#include "application.h"
#include "rpcheader.pb.h"
#include "zookeeperutil.h"
#include "coroutine/io_uring_awaiter.h"

#include <google/protobuf/stubs/callback.h>

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>

namespace
{
bool setNonBlock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
    {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

bool setTcpNoDelay(int fd, bool on)
{
    int opt = on ? 1 : 0;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) != -1;
}
}

namespace {
constexpr int kEventShift = 48;
constexpr uintptr_t kPtrMask = (1ULL << kEventShift) - 1;

// 协议/背压上限：防止恶意或异常 peer 触发巨量分配、压垮线 程池与发送队列
constexpr uint32_t kMaxHeaderSize = 64 * 1024;          // RpcHeader 最大 64KB
constexpr uint32_t kMaxBodySize = 64u * 1024 * 1024;    // args 最大 64MB
constexpr int kMaxInflightPerConn = 256;                // 单连接最大在途请求数

// provided buffer ring：所有连接共享一池内核可选缓冲，
// recv 不再各自带 4KB buffer，空闲连接零缓冲占用
constexpr unsigned kBufRingEntries = 1024;  // 必须是 2 的幂
constexpr unsigned kBufSize = 4096;         // 单个 buffer 大小
constexpr int kBufGroupId = 1;              // buffer group 标识
}

__u64 UringRpcProvider::packUserData(ConnectionContext* ctx, OpType op) {
    return (reinterpret_cast<uintptr_t>(ctx) & kPtrMask)
         | (static_cast<uintptr_t>(op) << kEventShift);
}
ConnectionContext* UringRpcProvider::unpackCtx(__u64 ud) {
    return reinterpret_cast<ConnectionContext*>(ud & kPtrMask);
}
UringRpcProvider::OpType UringRpcProvider::unpackOp(__u64 ud) {
    return static_cast<OpType>(ud >> kEventShift);
}

UringRpcProvider::UringRpcProvider()
    : m_listenfd(-1), m_ringReady(false), m_acceptPending(false), m_notifyPending(false),
      m_notifyFd(-1), m_poolStop(false),m_connMgr(ConnectionManager())
{
    int ret = io_uring_queue_init(256, &m_ring, 0);
    if (ret == 0)
    {
        m_ringReady = true;
    }

    m_notifyFd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

    constexpr int kThreads = 4;
    for (int i = 0; i < kThreads; ++i)
    {
        m_threadPool.emplace_back(&UringRpcProvider::workerThread, this);
    }
}

UringRpcProvider::~UringRpcProvider()
{
    {
        std::lock_guard<std::mutex> lock(m_taskMutex);
        m_poolStop = true;
    }
    m_taskCv.notify_all();
    for (auto &t : m_threadPool)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    if (m_notifyFd != -1)
    {
        close(m_notifyFd);
        m_notifyFd = -1;
    }

    if (m_listenfd != -1)
    {
        close(m_listenfd);
        m_listenfd = -1;
    }

    if (m_bufRing != nullptr)
    {
        io_uring_free_buf_ring(&m_ring, m_bufRing, kBufRingEntries, kBufGroupId);
        m_bufRing = nullptr;
    }
    if (m_bufBase != nullptr)
    {
        free(m_bufBase);
        m_bufBase = nullptr;
    }

    if (m_ringReady)
    {
        io_uring_queue_exit(&m_ring);
    }
}



void UringRpcProvider::NotifyService(google::protobuf::Service *service)
{
    ServiceInfo service_info;

    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    std::string service_name = pserviceDesc->name();
    int method_count = pserviceDesc->method_count();

    LOG_INFO("service_name:%s", service_name.c_str());

    for (int i = 0; i < method_count; ++i)
    {
        const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        service_info.m_methodMap.insert({method_name, pmethodDesc});

        // LOG_INFO("method name:%s", method_name.c_str());
    }
    service_info.m_service = service;
    m_serviceMap.insert({service_name, service_info});
}

void UringRpcProvider::Run()
{
    if (!m_ringReady)
    {
        // LOG_ERROR("io_uring_queue_init error");
        exit(EXIT_FAILURE);
    }

    std::string ip = KimRpcApplication::GetInstance().GetConfig().load("rpcserverip");
    uint16_t port = stoi(KimRpcApplication::GetInstance().GetConfig().load("rpcserverport"));

    startListen(ip, port);
    registerServiceToZk(ip, port);

    // std::cout << "UringRpcProvider start service at ip:" << ip << " port:" << port << std::endl;
    LOG_INFO("UringRpcProvider start service at ip:%s port:%d", ip.c_str(), port);

    if (!setupBufRing())
    {
        LOG_ERROR("setup provided buffer ring failed");
        exit(EXIT_FAILURE);
    }

    if (!submitAccept())
    {
        exit(EXIT_FAILURE);
    }
    if (!submitNotifyRead())
    {
        exit(EXIT_FAILURE);
    }
    eventLoop();
}

void UringRpcProvider::startListen(const std::string &ip, uint16_t port)
{
    m_listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (m_listenfd == -1)
    {
        // LOG_ERROR("create listen socket error:%s", strerror(errno));
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(m_listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) <= 0)
    {
        // LOG_ERROR("invalid rpcserverip:%s", ip.c_str());
        exit(EXIT_FAILURE);
    }

    if (bind(m_listenfd, (sockaddr *)&server_addr, sizeof(server_addr)) == -1)
    {
        // LOG_ERROR("bind error:%s", strerror(errno));
        exit(EXIT_FAILURE);
    }

    if (listen(m_listenfd, SOMAXCONN) == -1)
    {
        // LOG_ERROR("listen error:%s", strerror(errno));
        exit(EXIT_FAILURE);
    }

    if (!setNonBlock(m_listenfd))
    {
        // LOG_ERROR("set listen socket nonblock error:%s", strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void UringRpcProvider::registerServiceToZk(const std::string &ip, uint16_t port)
{
    // ZkClient zkCli;
    // zkCli.Start();
    m_zkClient.Start(); 

    for (auto &sp : m_serviceMap)
    {
        std::string service_path = "/" + sp.first;
        m_zkClient.Create(service_path.c_str(), nullptr, 0);

        for (auto &mp : sp.second.m_methodMap)
        {
            std::string method_path = service_path + "/" + mp.first;
            char method_path_data[128] = {0};
            snprintf(method_path_data, sizeof(method_path_data), "%s:%d", ip.c_str(), port);
            m_zkClient.Create(method_path.c_str(), method_path_data, strlen(method_path_data), ZOO_EPHEMERAL);
        }
    }
}

bool UringRpcProvider::setupBufRing()
{
    int err = 0;
    m_bufRing = io_uring_setup_buf_ring(&m_ring, kBufRingEntries, kBufGroupId, 0, &err);
    if (m_bufRing == nullptr)
    {
        LOG_ERROR("io_uring_setup_buf_ring error:%s", strerror(-err));
        return false;
    }

    m_bufBase = malloc(static_cast<size_t>(kBufRingEntries) * kBufSize);
    if (m_bufBase == nullptr)
    {
        return false;
    }

    // 把全部 buffer 一次性交给内核
    unsigned mask = io_uring_buf_ring_mask(kBufRingEntries);
    for (unsigned i = 0; i < kBufRingEntries; ++i)
    {
        char *buf = static_cast<char *>(m_bufBase) + static_cast<size_t>(i) * kBufSize;
        io_uring_buf_ring_add(m_bufRing, buf, kBufSize, i, mask, i);
    }
    io_uring_buf_ring_advance(m_bufRing, kBufRingEntries);
    return true;
}

void UringRpcProvider::eventLoop()
{
    while (true)
    {
        int wait_ret = io_uring_submit_and_wait(&m_ring, 1);
        if (wait_ret < 0)
        {
            LOG_ERROR("io_uring_submit_and_wait error:%s", strerror(-wait_ret));
            continue;
        }

        // SQ has been flushed to kernel; retry deferred submissions
        if (m_acceptPending && submitAccept())
        {
            m_acceptPending = false;
        }
        if (m_notifyPending && submitNotifyRead())
        {
            m_notifyPending = false;
        }

        io_uring_cqe *cqe = nullptr;
        while (io_uring_peek_cqe(&m_ring, &cqe) == 0)
        {
            __u64 ud = cqe->user_data;
            int result = cqe->res;
            unsigned cqe_flags = cqe->flags;
            io_uring_cqe_seen(&m_ring, cqe);

            // 协程完成（io_uring awaiter，bit63 标记）：直接唤醒对应协程。
            // 当前 RPC 分发不走这条路（靠 eventfd 恢复），但保留它，业务后续若在
            // 共享 ring 上 co_await async_recv/send 等即可直接复用。
            if (kimrpc::coro::isCoroUserData(ud))
            {
                kimrpc::coro::resumeFromCqe(ud, result, cqe_flags);
                continue;
            }

            ConnectionContext* ctx = unpackCtx(ud);
            OpType op = unpackOp(ud);

            // RECV 携带内核选中的 provided buffer：无论连接是否正在关闭，
            // 都必须先把数据拷出、buffer 归还 ring，否则关连接时会泄漏内核缓冲。
            // 注意：拷贝必须在归还之前，否则内核可能用新数据覆盖该 buffer。
            if (op == OpType::RECV && (cqe_flags & IORING_CQE_F_BUFFER))
            {
                unsigned bid = cqe_flags >> IORING_CQE_BUFFER_SHIFT;
                char *buf = static_cast<char *>(m_bufBase) + static_cast<size_t>(bid) * kBufSize;
                if (result > 0 && ctx != nullptr && !ctx->closing)
                {
                    ctx->input.append(buf, result);
                }
                io_uring_buf_ring_add(m_bufRing, buf, kBufSize, bid,
                                      io_uring_buf_ring_mask(kBufRingEntries), 0);
                io_uring_buf_ring_advance(m_bufRing, 1);
            }

            if (m_connMgr.onComplete(ctx))
            {
                continue;
            }

            switch (op)
            {
            case OpType::ACCEPT:
                handleAccept(result);
                break;
            case OpType::RECV:
                handleRecv(ctx, result);
                break;
            case OpType::SEND:
                handleSend(ctx, result);
                break;
            case OpType::NOTIFY:
                handleNotify();
                if (!submitNotifyRead())
                {
                    m_notifyPending = true;
                }
                break;
            }
        }
    }
}

bool UringRpcProvider::submitAccept()
{
    io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr)
    {
        return false;
    }
    io_uring_prep_accept(sqe, m_listenfd, nullptr,nullptr, 0);
    sqe->user_data = packUserData(nullptr, OpType::ACCEPT);
    return true;
}

bool UringRpcProvider::submitRecv(int fd)
{
    ConnectionContext *ctx = m_connMgr.find(fd);
    if (ctx == nullptr)return false;
    io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr)
    {
        return false;
    }
    // 不再指定缓冲：由内核从 buffer group 选一个 provided buffer 填充，
    // 完成时通过 cqe->flags 告知 buffer id
    io_uring_prep_recv(sqe, fd, nullptr, kBufSize, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = kBufGroupId;
    io_uring_sqe_set_data64(sqe, packUserData(ctx, OpType::RECV));
    m_connMgr.onSubmit(ctx);
    return true;
}

// 为队首响应启动/继续一次 send；队列空则将 sending 置 false。SQ 满返回 false
bool UringRpcProvider:: submitSend(int fd)
{
    ConnectionContext *ctx = m_connMgr.find(fd);
    if (ctx == nullptr) return false;

    if (ctx->outQueue.empty())
    {
        ctx->sending = false;
        return true;
    }

    io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr)
    {
        return false;
    }

    const std::string &front = ctx->outQueue.front();
    const char *data = front.data() + ctx->written;
    size_t len = front.size() - ctx->written;
    io_uring_prep_send(sqe, fd, data, len, 0);
    sqe->user_data = packUserData(ctx, OpType::SEND);
    m_connMgr.onSubmit(ctx);
    ctx->sending = true;
    return true;
}

bool UringRpcProvider::submitNotifyRead()
{
    io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr)
    {
        return false;
    }
    
    io_uring_prep_read(sqe, m_notifyFd, m_notifyBuf, sizeof(m_notifyBuf), 0);
    sqe->user_data = packUserData(nullptr, OpType::NOTIFY);
    return true;
}

void UringRpcProvider::handleNotify()
{
    // worker 线程完成 CallMethod 后把协程句柄投到这里，统一在 reactor 线程恢复。
    // 恢复后协程会走到 co_await 之后，把成帧响应交给 deliverResponse 发送。
    std::vector<std::coroutine_handle<>> ready;
    {
        std::lock_guard<std::mutex> lock(m_readyMutex);
        ready.swap(m_readyCoros);
    }
    for (auto h : ready)
    {
        h.resume();
    }
}

// reactor 线程：connId 校验后把成帧响应推入发送队列，必要时启动发送链。
// 与原 handleNotify 的投递逻辑一致，复用现有的串行发送 + 背压机制。
void UringRpcProvider::deliverResponse(int fd, uint64_t connId, std::string data)
{
    ConnectionContext *ctx = m_connMgr.find(fd);
    // fd 已被新连接复用：connId 对不上，丢弃这条属于旧连接的响应
    if (ctx == nullptr || ctx->connId != connId)
    {
        return;
    }
    ctx->outQueue.push_back(std::move(data));
    // 已有 send 在途时，后续响应由 handleSend 链式取走；否则在此启动
    if (!ctx->sending)
    {
        if (!submitSend(fd))
        {
            LOG_ERROR("SQ full, cannot send response on fd=%d, closing", fd);
            m_connMgr.close(fd);
        }
    }
}

void UringRpcProvider::resumeOnReactor(std::coroutine_handle<> h)
{
    {
        std::lock_guard<std::mutex> lock(m_readyMutex);
        m_readyCoros.push_back(h);
    }
    uint64_t val = 1;
    ::write(m_notifyFd, &val, sizeof(val));
}

void UringRpcProvider::workerThread()
{
    while (true)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(m_taskMutex);
            m_taskCv.wait(lock, [this] { return m_poolStop || !m_taskQueue.empty(); });
            if (m_poolStop && m_taskQueue.empty())
            {
                return;
            }
            task = std::move(m_taskQueue.front());
            m_taskQueue.pop();
        }
        task();
    }
}

void UringRpcProvider::submitTask(std::function<void()> task)
{
    {
        std::lock_guard<std::mutex> lock(m_taskMutex);
        m_taskQueue.push(std::move(task));
    }
    m_taskCv.notify_one();
}

void UringRpcProvider::handleAccept(int result)
{
    if (result < 0)
    {
        LOG_ERROR("accept error:%s", strerror(-result));
        if (!submitAccept())
        {
            m_acceptPending = true;
        }
        return;
    }

    int connfd = result;
    if (!setTcpNoDelay(connfd, true))
    {
        LOG_ERROR("set TCP_NODELAY error on fd=%d:%s", connfd, strerror(errno));
    }
    setNonBlock(connfd);
    m_connMgr.acquire(connfd);

    if (!submitRecv(connfd))
    {
        LOG_ERROR("SQ full, cannot recv on fd=%d, closing", connfd);
        m_connMgr.close(connfd); 
    }
    if (!submitAccept())
    {
        m_acceptPending = true;
    }
}

void UringRpcProvider::handleRecv(ConnectionContext *ctx, int result)
{
    int fd = ctx->fd;

    // 连接正在关闭（buffer 已在 eventLoop 归还）：不再读/派发新请求，等在途 op 排空回收
    if (ctx->closing)
    {
        return;
    }

    if (result <= 0)
    {
        if (result == -ENOBUFS)
        {
            // buffer ring 暂时取空（非错误）：重挂 recv，缓冲会随处理回填
            if (!submitRecv(fd))
            {
                LOG_ERROR("SQ full on ENOBUFS rearm fd=%d, closing", fd);
                m_connMgr.close(fd);
            }
            return;
        }
        if (result < 0)
        {
            LOG_ERROR("recv error:%s", strerror(-result));
        }
        m_connMgr.close(fd);  // 0 = EOF / 其它错误
        return;
    }

    // 数据已在 eventLoop 中从 provided buffer 拷入 ctx->input
    // 排空缓冲区里所有完整请求（多路复用：一次 recv 可能带回多个流水线请求）
    while (tryHandleRequest(fd, ctx))
    {
        // tryHandleRequest 在出错时会关闭连接并返回 true，此时 ctx 已不可再用
        if (m_connMgr.find(fd) == nullptr)
        {
            return;
        }
    }

    // 背压：在途请求达到上限时暂停 recv，不再从该连接读入更多请求，
    // 让 TCP 接收窗口自然回压客户端；待响应发完后在 handleSend 里恢复
    if (ctx->pendingReqs >= kMaxInflightPerConn)
    {
        ctx->readPaused = true;
        return;
    }

    // recv 与 send 解耦：排空后立即重新挂 recv，不再等响应发完
    if (!submitRecv(fd))
    {
        LOG_ERROR("SQ full, cannot recv on fd=%d, closing", fd);
        m_connMgr.close(fd);
    }
}

void UringRpcProvider::handleSend(ConnectionContext *ctx, int result)
{
    int fd = ctx->fd;
    if (result <= 0)
    {
        if (result < 0)
        {
            LOG_ERROR("send error:%s", strerror(-result));
        }
        m_connMgr.close(fd);
        return;
    }

    ctx->written += result;
    // 队首响应发完则弹出，准备发下一个
    if (ctx->written >= ctx->outQueue.front().size())
    {
        ctx->outQueue.pop_front();
        ctx->written = 0;
        --ctx->pendingReqs;  // 一条请求的响应彻底发完，退出在途计数

        // 背压解除：若此前因在途上限暂停了 recv，现在降到阈值下则恢复读
        if (ctx->readPaused && ctx->pendingReqs < kMaxInflightPerConn)
        {
            ctx->readPaused = false;
            if (!submitRecv(fd))
            {
                LOG_ERROR("SQ full, cannot resume recv on fd=%d, closing", fd);
                m_connMgr.close(fd);
                return;  // ctx 可能已回收，勿再续发
            }
        }
    }

    if (ctx->outQueue.empty())
    {
        ctx->sending = false; // 没有更多响应，停止发送链
        return;
    }

    if (!submitSend(fd))
    {
        LOG_ERROR("SQ full, cannot continue send on fd=%d, closing", fd);
        m_connMgr.close(fd);
    }
}

bool UringRpcProvider::tryHandleRequest(int fd, ConnectionContext *ctx)
{
    if (ctx->input.size() < sizeof(uint32_t))
    {
        return false;
    }

    uint32_t header_size = 0;
    ctx->input.copy((char *)&header_size, sizeof(uint32_t), 0);

    // 先校验长度上限，再决定是否继续攒数据：否则恶意的超大 header_size
    // 会让 input 无上限增长（永远等不齐）
    if (header_size > kMaxHeaderSize)
    {
        LOG_ERROR("header_size %u exceeds limit on fd=%d, closing", header_size, fd);
        m_connMgr.close(fd);
        return true;
    }

    if (ctx->input.size() < sizeof(uint32_t) + header_size)
    {
        return false;
    }

    std::string rpc_header_str = ctx->input.substr(sizeof(uint32_t), header_size);
    KimRpc::RpcHeader rpcHeader;
    if (!rpcHeader.ParseFromString(rpc_header_str))
    {
        LOG_ERROR("rpc_header_str:%s parse error!", rpc_header_str.c_str());
        m_connMgr.close(fd);
        return true;
    }

    std::string service_name = rpcHeader.service_name();
    std::string method_name = rpcHeader.method_name();
    uint32_t args_size = rpcHeader.args_size();
    uint64_t request_id = rpcHeader.request_id();

    if (args_size > kMaxBodySize)
    {
        LOG_ERROR("args_size %u exceeds limit on fd=%d, closing", args_size, fd);
        m_connMgr.close(fd);
        return true;
    }

    size_t request_size = sizeof(uint32_t) + header_size + args_size;

    if (ctx->input.size() < request_size)
    {
        return false;
    }

    std::string args_str = ctx->input.substr(sizeof(uint32_t) + header_size, args_size);
    ctx->input.erase(0, request_size);

    // std::cout << "==================================" << std::endl;
    // std::cout << "header_size:" << header_size << std::endl;
    // std::cout << "rpc_header_str:" << rpc_header_str << std::endl;
    // std::cout << "service_name:" << service_name << std::endl;
    // std::cout << "method_name:" << method_name << std::endl;
    // std::cout << "args_str:" << args_str << std::endl;
    // std::cout << "args_size:" << args_size << std::endl;
    // std::cout << "==================================" << std::endl;

    auto it = m_serviceMap.find(service_name);
    if (it == m_serviceMap.end())
    {
        LOG_ERROR("%s is not exist!", service_name.c_str());
        m_connMgr.close(fd);
        return true;
    }

    ServiceInfo &sinfo = it->second;
    auto mit = sinfo.m_methodMap.find(method_name);
    if (mit == sinfo.m_methodMap.end())
    {
        LOG_ERROR("%s:%s is not exist!", service_name.c_str(), method_name.c_str());
        m_connMgr.close(fd);
        return true;
    }

    google::protobuf::Service *service = sinfo.m_service;
    const google::protobuf::MethodDescriptor *method = mit->second;

    google::protobuf::Message *request = service->GetRequestPrototype(method).New();
    if (!request->ParseFromString(args_str))
    {
        LOG_ERROR("request parse error! content: %s", args_str.c_str());
        delete request;
        m_connMgr.close(fd);
        return true;
    }

    ++ctx->pendingReqs;  // 派发即计入在途，响应发完后在 handleSend 里递减
    // 每个请求起一个协程：DetachedTask 即发即忘，运行到 co_await（offload 到线程池）
    // 即让出 reactor 线程；response 在协程内创建，request 所有权转交协程。
    handleRpc(fd, ctx->connId, request_id, service, method, request);
    return true;
}

// 每个请求的协程主体（在 reactor 线程启动）
kimrpc::coro::DetachedTask UringRpcProvider::handleRpc(
    int fd, uint64_t connId, uint64_t request_id,
    google::protobuf::Service *service,
    const google::protobuf::MethodDescriptor *method,
    google::protobuf::Message *request)
{
    google::protobuf::Message *response = service->GetResponsePrototype(method).New();

    // frame 是协程局部：onPoolDone 在 worker 线程经 &frame 写入成帧响应；不从
    // await_resume 按值返回 std::string（规避 GCC 11 协程 codegen bug，详见 awaiter 注释）。
    std::string frame;
    co_await CallOnPoolAwaiter{this, service, method, request, response, request_id, &frame};

    // 已回到 reactor 线程：交给现有发送机制（connId 校验 + 串行发送 + 背压）
    if (!frame.empty())
    {
        deliverResponse(fd, connId, std::move(frame));
    }
    co_return;
}

// awaiter：把 CallMethod 提交到线程池执行。done 回调绑定 awaiter 与协程句柄，
// 业务执行完毕调用 done->Run() 时触发 onPoolDone（在 worker 线程）。
void UringRpcProvider::CallOnPoolAwaiter::await_suspend(std::coroutine_handle<> h)
{
    google::protobuf::Closure *done =
        google::protobuf::NewCallback(self, &UringRpcProvider::onPoolDone, this, h);

    // 全部按值捕获，task 内不再触碰 awaiter：协程一旦恢复完成，awaiter 即随帧销毁
    google::protobuf::Service *svc = service;
    const google::protobuf::MethodDescriptor *m = method;
    google::protobuf::Message *req = request;
    google::protobuf::Message *resp = response;
    self->submitTask([svc, m, req, resp, done]() {
        svc->CallMethod(m, nullptr, req, resp, done);
        delete req;  // request 仅在 CallMethod 期间使用，返回后即可释放
    });
}

// 业务 done 回调（worker 线程）：序列化响应写入 *aw->out（协程局部 frame），释放
// response，再把协程句柄投回 reactor 线程恢复。序列化放在 worker 线程，避免 reactor
// 与 worker 跨线程读 response 的数据竞争。
void UringRpcProvider::onPoolDone(CallOnPoolAwaiter *aw, std::coroutine_handle<> h)
{
    std::string response_str;
    if (aw->response->SerializeToString(&response_str))
    {
        // wire: [4字节 size][8字节 request_id][body], size = 8 + body
        uint32_t response_size = sizeof(uint64_t) + response_str.size();
        std::string &frame = *aw->out; // 写协程局部变量
        frame.reserve(sizeof(uint32_t) + response_size);
        frame.append(reinterpret_cast<char *>(&response_size), sizeof(uint32_t));
        frame.append(reinterpret_cast<char *>(&aw->request_id), sizeof(uint64_t));
        frame += response_str;
    }
    else
    {
        LOG_ERROR("serialize response_str error");
        // frame 留空：handleRpc 恢复后跳过发送（响应丢失，连接保持）
    }
    delete aw->response;

    // frame 写入 happens-before resumeOnReactor 里的入队，与 reactor 线程取队
    // 构成同步：协程恢复后能安全看到 frame。此后不再触碰 aw。
    resumeOnReactor(h);
}


