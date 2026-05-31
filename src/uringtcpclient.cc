#include "uringtcpclient.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace uringtcpclient
{
bool waitUringResult(io_uring *ring, int *result, std::string *error, const char *op)
{
    int submit_ret = io_uring_submit(ring);
    if (submit_ret < 0)
    {
        if (error != nullptr)
        {
            *error = std::string(op) + " submit error: " + strerror(-submit_ret);
        }
        return false;
    }

    io_uring_cqe *cqe = nullptr;
    int wait_ret = io_uring_wait_cqe(ring, &cqe);
    if (wait_ret < 0)
    {
        if (error != nullptr)
        {
            *error = std::string(op) + " wait cqe error: " + strerror(-wait_ret);
        }
        return false;
    }

    *result = cqe->res;
    io_uring_cqe_seen(ring, cqe);
    if (*result < 0)
    {
        if (error != nullptr)
        {
            *error = std::string(op) + " error: " + strerror(-(*result));
        }
        return false;
    }
    return true;
}
}

UringTcpClient::UringTcpClient()
    : m_fd(-1), m_ringReady(false)
{
    int ret = io_uring_queue_init(64, &m_ring, 0);
    if (ret == 0)
    {
        m_ringReady = true;
    }
}

UringTcpClient::~UringTcpClient()
{
    Close();
    if (m_ringReady)
    {
        io_uring_queue_exit(&m_ring);
    }
}

bool UringTcpClient::AsyncConnect(const std::string &ip, uint16_t port, UringCallback callback)
{
    if (!m_ringReady)
    {
        if (callback)
        {
            callback(false, 0, "io_uring queue init error");
        }
        return false;
    }
    m_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (m_fd == -1)
    {
        return false;
    }
    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) <= 0)
    {
        if (callback)
        {
            callback(false, 0, "invalid server ip: " + ip);
        }
        return false;
    }
    io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr)
    {
        if (callback)
        {
            callback(false, 0, "get connect sqe error");
        }
        Close();
        return false;
    } 
    io_uring_prep_connect(sqe, m_fd, (sockaddr *)&server_addr, sizeof(server_addr));
    IoContext *ctx = new IoContext{OpType::Connect, m_fd, callback, "", {0}};
    io_uring_sqe_set_data(sqe, ctx);
    int ret = io_uring_submit(&m_ring);
    if (ret < 0)
    {
        if (callback)        
        {
            callback(false, 0, std::string("connect submit error: ") + strerror(-ret));
        }
        Close();
        return false;
    }
    return true;
}

bool UringTcpClient::AsyncWrite(const std::string &data, UringCallback callback)
{
    if (m_fd == -1)
    {
        if (callback)
        {
            callback(false, 0, "socket is not connected");
        }
        return false;
    }
    io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr)
    {
        if (callback)
        {
            callback(false, 0, "get send sqe error");
        }
        return false;
    }
    IoContext *ctx = new IoContext{OpType::Send, m_fd, callback, std::string(data), {0}};
    io_uring_prep_send(sqe, m_fd, ctx->write_buf.data(), ctx->write_buf.size(), 0);

    io_uring_sqe_set_data(sqe, ctx);
    int ret = io_uring_submit(&m_ring);
    if (ret < 0)
    {
        if (callback)
        {
            callback(false, 0, std::string("send submit error: ") + strerror(-ret));
        }
        return false;
    }
    return true;
}

bool UringTcpClient::AsyncRead(UringCallback callback)
{
    if (m_fd == -1)
    {
        if (callback)
        {
            callback(false, 0, "socket is not connected");
        }
        return false;
    }
    io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr)
    {
        if (callback)
        {
            callback(false, 0, "get recv sqe error");
        }
        return false;
    }
    IoContext *ctx = new IoContext{OpType::Recv, m_fd, callback, "", {0}};
    io_uring_prep_recv(sqe, m_fd, ctx->read_buf, sizeof(ctx->read_buf), 0);
    io_uring_sqe_set_data(sqe, ctx);
    int ret = io_uring_submit(&m_ring);
    if (ret < 0)
    {
        if (callback)
        {
            callback(false, 0, std::string("recv submit error: ") + strerror(-ret));
        }
        return false;
    }
    return true;
}

void UringTcpClient::EventLoop()
{
    if (!m_ringReady)
    {
        return;
    }
    io_uring_cqe *cqe;
    while (true)
    {
        int ret = io_uring_wait_cqe(&m_ring, &cqe);
        if (ret < 0)
        {
            continue;
        }

        IoContext *ctx = (IoContext *)io_uring_cqe_get_data(cqe);
        if (ctx->opType == OpType::Connect)
        {
            if (ctx->callback)
            {
                if (cqe->res < 0)
                {
                    ctx->callback(false, cqe->res, std::string("connect error: ") + strerror(-cqe->res));
                }
                else
                {
                    ctx->callback(true, cqe->res, "");
                }
            }
        }
        else if (ctx->opType == OpType::Send)
        {
            if (ctx->callback)
            {
                if (cqe->res < 0)
                {
                    ctx->callback(false, cqe->res, std::string("send error: ") + strerror(-cqe->res));
                }
                else
                {
                    ctx->callback(true, cqe->res, "");
                }
            }
        }
        else if (ctx->opType == OpType::Recv)
        {
            if (ctx->callback)
            {
                if (cqe->res < 0)
                {
                    ctx->callback(false, cqe->res, std::string("recv error: ") + strerror(-cqe->res));
                }
                else
                {
                    ctx->callback(true, cqe->res, std::string(ctx->read_buf, cqe->res));
                }
            }
        }
        delete ctx;
        io_uring_cqe_seen(&m_ring, cqe);
    }
}

bool UringTcpClient::Connect(const std::string &ip, uint16_t port, std::string *error)
{
    if (!m_ringReady)
    {
        if (error != nullptr)
        {
            *error = "io_uring queue init error";
        }
        return false;
    }

    if (m_fd != -1)
    {
        Close();
    }

    m_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (m_fd == -1)
    {
        if (error != nullptr)
        {
            *error = std::string("create socket error: ") + strerror(errno);
        }
        return false;
    }

    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) <= 0)
    {
        if (error != nullptr)
        {
            *error = "invalid server ip: " + ip;
        }
        Close();
        return false;
    }

    io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr)
    {
        if (error != nullptr)
        {
            *error = "get connect sqe error";
        }
        Close();
        return false;
    }

    io_uring_prep_connect(sqe, m_fd, (sockaddr *)&server_addr, sizeof(server_addr));
    int result = 0;
    if (!uringtcpclient::waitUringResult(&m_ring, &result, error, "connect"))
    {
        Close();
        return false;
    }
    return true;
}

bool UringTcpClient::WriteAll(const char *data, size_t len, std::string *error)
{
    if (m_fd == -1)
    {
        if (error != nullptr)
        {
            *error = "socket is not connected";
        }
        return false;
    }

    size_t written = 0;
    while (written < len)
    {
        io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
        if (sqe == nullptr)
        {
            if (error != nullptr)
            {
                *error = "get send sqe error";
            }
            return false;
        }

        io_uring_prep_send(sqe, m_fd, data + written, len - written, 0);
        int result = 0;
        if (!uringtcpclient::waitUringResult(&m_ring, &result, error, "send"))
        {
            return false;
        }
        if (result == 0)
        {
            if (error != nullptr)
            {
                *error = "send error: connection closed";
            }
            return false;
        }
        written += result;
    }
    return true;
}

bool UringTcpClient::ReadSome(std::string *out, std::string *error)
{
    if (m_fd == -1)
    {
        if (error != nullptr)
        {
            *error = "socket is not connected";
        }
        return false;
    }

    out->clear();
    char buf[4096] = {0};
    while (true)
    {
        io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
        if (sqe == nullptr)
        {
            if (error != nullptr)
            {
                *error = "get recv sqe error";
            }
            return false;
        }

        io_uring_prep_recv(sqe, m_fd, buf, sizeof(buf), 0);
        int result = 0;
        if (!uringtcpclient::waitUringResult(&m_ring, &result, error, "recv"))
        {
            return false;
        }
        if (result == 0)
        {
            break;
        }
        out->append(buf, result);
    }

    if (out->empty())
    {
        if (error != nullptr)
        {
            *error = "recv error: empty response";
        }
        return false;
    }
    return true;
}

bool UringTcpClient::ReadExact(char *data, size_t len, std::string *error)
{
    if (m_fd == -1)
    {
        if (error != nullptr)
        {
            *error = "socket is not connected";
        }
        return false;
    }

    size_t received = 0;
    while (received < len)
    {
        io_uring_sqe *sqe = io_uring_get_sqe(&m_ring);
        if (sqe == nullptr)
        {
            if (error != nullptr)
            {
                *error = "get recv sqe error";
            }
            return false;
        }

        io_uring_prep_recv(sqe, m_fd, data + received, len - received, 0);
        int result = 0;
        if (!uringtcpclient::waitUringResult(&m_ring, &result, error, "recv"))
        {
            return false;
        }
        if (result == 0)
        {
            if (error != nullptr)
            {
                *error = "recv error: connection closed";
            }
            return false;
        }
        received += result;
    }
    return true;
}

void UringTcpClient::Close()
{
    if (m_fd != -1)
    {
        close(m_fd);
        m_fd = -1;
    }
}
