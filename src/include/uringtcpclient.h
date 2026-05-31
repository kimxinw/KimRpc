#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <liburing.h>
#include <string>

enum class OpType
{
    Connect,
    Send,
    Recv
};

using UringCallback = std::function<void(bool success, int result, const std::string &error)>;

struct IoContext // io_uring提交队列项的上下文
{
    OpType opType;
    int fd;
    UringCallback callback;
    std::string write_buf;
    char read_buf[4096];
};

class UringTcpClient
{
public:
    UringTcpClient();
    ~UringTcpClient();

    UringTcpClient(const UringTcpClient &) = delete;
    UringTcpClient &operator=(const UringTcpClient &) = delete;

    bool Connect(const std::string &ip, uint16_t port, std::string *error);
    bool WriteAll(const char *data, size_t len, std::string *error);
    bool ReadExact(char *data, size_t len, std::string *error);
    bool ReadSome(std::string *out, std::string *error);
    void Close();
    bool IsConnected() const { return m_fd != -1; }
    bool AsyncConnect(const std::string &ip, uint16_t port, UringCallback callback);
    bool AsyncWrite(const std::string &data, UringCallback callback);
    bool AsyncRead(UringCallback callback);
    void EventLoop(); // 处理io_uring事件

private:
    int m_fd;
    io_uring m_ring;
    bool m_ringReady;
};
