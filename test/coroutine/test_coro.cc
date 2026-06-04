// 协程模块自测：脱离 UringRpcProvider，用一个独立 ring 验证
//   1) Task<T> 的 co_await 串联 + 返回值传递
//   2) io_uring awaiter 真正挂起/恢复（用 pipe 做异步读写）
//   3) DetachedTask 即发即忘
//
// 编译（也可由 CMake 生成 test_coro 目标）：
//   g++ -std=c++20 -I../../src/include test_coro.cc -luring -o test_coro

#include "coroutine/io_uring_awaiter.h"
#include "coroutine/sync_wait.h"
#include "coroutine/task.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <liburing.h>
#include <unistd.h>

using namespace kimrpc::coro;

// 纯计算的子协程：验证返回值与 co_await 串联
Task<int> add(int a, int b)
{
    co_return a + b;
}

Task<int> compute()
{
    int x = co_await add(2, 3);
    int y = co_await add(x, 10);
    co_return x + y; // 5 + 15 = 20
}

// 真正的异步 IO：往 pipe 写，再异步 recv/read 读回来
Task<std::string> echoThroughPipe(io_uring *ring, int rfd, int wfd)
{
    const char msg[] = "hello-coro";
    // 同步写入 pipe（小数据，不会阻塞），随后异步读
    ssize_t w = ::write(wfd, msg, sizeof(msg) - 1);
    assert(w == sizeof(msg) - 1);

    char buf[64] = {0};
    int n = co_await async_read(ring, rfd, buf, sizeof(buf));
    if (n < 0)
    {
        co_return std::string("read error: ") + std::strerror(-n);
    }
    co_return std::string(buf, n);
}

DetachedTask fireAndForget(bool *ran)
{
    *ran = true;
    co_return;
}

int main()
{
    io_uring ring;
    if (io_uring_queue_init(8, &ring, 0) != 0)
    {
        std::perror("io_uring_queue_init");
        return 1;
    }

    // 1) 纯计算协程
    int r = sync_wait(&ring, compute());
    std::printf("compute() = %d (expect 20)\n", r);
    assert(r == 20);

    // 2) 异步 IO 协程
    int fds[2];
    if (::pipe(fds) != 0)
    {
        std::perror("pipe");
        return 1;
    }
    std::string echoed = sync_wait(&ring, echoThroughPipe(&ring, fds[0], fds[1]));
    std::printf("echoThroughPipe() = \"%s\" (expect \"hello-coro\")\n", echoed.c_str());
    assert(echoed == "hello-coro");
    ::close(fds[0]);
    ::close(fds[1]);

    // 3) DetachedTask 即发即忘
    bool ran = false;
    fireAndForget(&ran);
    std::printf("DetachedTask ran = %d (expect 1)\n", ran);
    assert(ran);

    io_uring_queue_exit(&ring);
    std::printf("ALL COROUTINE TESTS PASSED\n");
    return 0;
}
