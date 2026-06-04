#pragma once

// KimRpc 协程模块 —— sync_wait
//
// 在一个给定的 io_uring 上，同步驱动一个 Task 跑到完成并取回结果。
// 它自己跑一个最小事件循环：提交并等待 CQE，把带协程标记的 CQE 唤回对应协程。
//
// 用途：
//   - 单元测试 / 调试：脱离 UringRpcProvider 单独验证协程逻辑（见 test/coroutine）。
//   - 在没有常驻 reactor 的上下文里临时把一段异步链跑成同步。
//
// 注意：它会消费该 ring 上的“所有” CQE。如果该 ring 同时还跑着别的（非协程）
// 操作，这个简易循环并不分发它们——所以 sync_wait 适合用在“专门为这次调用准备
// 的 ring”上，而不是生产环境那个共享 ring（生产环境由 provider 的 eventLoop 分发）。

#include "io_uring_awaiter.h"
#include "task.h"

#include <liburing.h>
#include <type_traits>

namespace kimrpc::coro
{

namespace detail
{
// 把 ring 上已就绪的 CQE 全部取出并唤醒对应协程
inline void drainCqes(io_uring *ring)
{
    io_uring_submit_and_wait(ring, 1);

    io_uring_cqe *cqe = nullptr;
    unsigned head = 0;
    unsigned count = 0;
    io_uring_for_each_cqe(ring, head, cqe)
    {
        ++count;
        __u64 ud = cqe->user_data;
        if (isCoroUserData(ud))
        {
            // 先把 res/flags 拷出，再统一 advance：resume 时不可继续访问 cqe 内存
            int res = cqe->res;
            unsigned flags = cqe->flags;
            resumeFromCqe(ud, res, flags);
        }
        // 非协程 CQE 在 sync_wait 场景下忽略
    }
    io_uring_cq_advance(ring, count);
}
} // namespace detail

template <typename T>
T sync_wait(io_uring *ring, Task<T> task)
{
    auto handle = task.handle();
    handle.resume(); // Task 惰性：手动启动，跑到第一个挂起点

    while (!handle.done())
    {
        detail::drainCqes(ring);
    }

    if constexpr (std::is_void_v<T>)
    {
        handle.promise().result(); // 仅为重抛可能的异常
    }
    else
    {
        return std::move(handle.promise()).result();
    }
}

} // namespace kimrpc::coro
