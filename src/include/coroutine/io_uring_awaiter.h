#pragma once

// KimRpc 协程模块 —— io_uring awaiter
//
// 把一次 io_uring 异步操作包装成可 co_await 的对象：
//
//   int n = co_await async_recv(ring, fd, buf, len);
//
// 语义：在当前协程里 prep 一个 SQE 挂到 ring 上，挂起协程；待对应 CQE 回来时，
// 事件循环调用 resumeFromCqe() 唤醒本协程，await_resume 返回 cqe->res
// （>0 字节数 / 0 EOF / <0 -errno），与裸 io_uring 完全一致。
//
// ── 与现有 user_data 方案的兼容 ────────────────────────────────────────────
// UringRpcProvider 现在把 user_data 打包成 (ctx 指针 & 低48位) | (op << 48)，
// op 取值 0..3（落在 bit 48~49）。本模块用 **bit 63** 作为“协程完成”标记，
// 两者互不冲突。事件循环里集成时只需在最前面加：
//
//   if (kimrpc::coro::isCoroUserData(ud)) {
//       kimrpc::coro::resumeFromCqe(ud, result, cqe_flags);
//       continue;                 // 已在 cqe 取出后 seen
//   }
//
// 用户态指针在 x86-64 上 < 2^47，bit 63 恒为 0，故可安全借用。

#include "task.h"

#include <liburing.h>
#include <cerrno>
#include <coroutine>
#include <cstdint>

namespace kimrpc::coro
{

// 跨越挂起点存活的 IO 请求状态。它是 awaiter 的成员，而 awaiter 是 co_await
// 表达式的临时对象——C++ 标准保证其生命周期横跨整个挂起，故 &req 在 resume 前始终有效。
struct IoRequest
{
    std::coroutine_handle<> handle{};
    int result = 0;       // cqe->res
    unsigned cqe_flags = 0; // cqe->flags（如 provided buffer 的 buffer id）
};

// ── user_data 标记：bit 63 = 协程完成 ──────────────────────────────────────
inline constexpr __u64 kCoroTagBit = 1ULL << 63;

inline __u64 makeCoroUserData(IoRequest *req) noexcept
{
    return reinterpret_cast<__u64>(req) | kCoroTagBit;
}
inline bool isCoroUserData(__u64 ud) noexcept
{
    return (ud & kCoroTagBit) != 0;
}
inline IoRequest *coroRequestFromUserData(__u64 ud) noexcept
{
    return reinterpret_cast<IoRequest *>(ud & ~kCoroTagBit);
}

// 事件循环收到带协程标记的 CQE 时调用：写回结果并唤醒挂起的协程。
// 调用前请先 io_uring_cqe_seen()，因为 resume 可能立刻又往 ring 里塞新的 SQE。
inline void resumeFromCqe(__u64 ud, int res, unsigned flags) noexcept
{
    IoRequest *req = coroRequestFromUserData(ud);
    req->result = res;
    req->cqe_flags = flags;
    req->handle.resume();
}

// 通用 awaiter：PrepFn 是一个 void(io_uring_sqe*) 可调用对象，负责把本次操作
// prep 到 sqe 上（io_uring_prep_recv / _send / _accept ...）。
// 用模板而非 std::function，避免每次 IO 的堆分配。
template <typename PrepFn>
struct UringAwaiter
{
    io_uring *ring;
    PrepFn prep;
    IoRequest req{};

    bool await_ready() const noexcept { return false; }

    // 返回 true：已挂上 SQE，等 CQE 唤醒；返回 false：拿不到 SQE，立即恢复并把
    // result 置为 -ENOMEM，让协程自己决定怎么处理（不会永久挂死）。
    bool await_suspend(std::coroutine_handle<> h) noexcept
    {
        io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (sqe == nullptr)
        {
            io_uring_submit(ring); // 先把已攒的 SQE 刷给内核，腾出 SQ 空间
            sqe = io_uring_get_sqe(ring);
        }
        if (sqe == nullptr)
        {
            req.result = -ENOMEM; // SQ 仍满：不挂起，立即返回错误
            return false;
        }
        req.handle = h;
        prep(sqe);
        io_uring_sqe_set_data64(sqe, makeCoroUserData(&req));
        return true;
    }

    // 实际提交时机：交给事件循环的 io_uring_submit_and_wait 批量 flush，
    // 这里不单独 submit，以便多个 co_await 的 SQE 合批。
    int await_resume() const noexcept { return req.result; }

    // 取回 CQE flags（例如 provided buffer 的 buffer id），需要时单独读
    unsigned flags() const noexcept { return req.cqe_flags; }
};

template <typename PrepFn>
UringAwaiter<PrepFn> make_uring_awaitable(io_uring *ring, PrepFn &&prep) noexcept
{
    return UringAwaiter<PrepFn>{ring, std::forward<PrepFn>(prep), {}};
}

// ── 常用操作的便捷封装 ─────────────────────────────────────────────────────

inline auto async_recv(io_uring *ring, int fd, void *buf, unsigned len, int flags = 0) noexcept
{
    return make_uring_awaitable(ring, [=](io_uring_sqe *sqe) {
        io_uring_prep_recv(sqe, fd, buf, len, flags);
    });
}

inline auto async_send(io_uring *ring, int fd, const void *buf, unsigned len, int flags = 0) noexcept
{
    return make_uring_awaitable(ring, [=](io_uring_sqe *sqe) {
        io_uring_prep_send(sqe, fd, buf, len, flags);
    });
}

inline auto async_read(io_uring *ring, int fd, void *buf, unsigned len, __u64 offset = 0) noexcept
{
    return make_uring_awaitable(ring, [=](io_uring_sqe *sqe) {
        io_uring_prep_read(sqe, fd, buf, len, offset);
    });
}

inline auto async_write(io_uring *ring, int fd, const void *buf, unsigned len, __u64 offset = 0) noexcept
{
    return make_uring_awaitable(ring, [=](io_uring_sqe *sqe) {
        io_uring_prep_write(sqe, fd, buf, len, offset);
    });
}

inline auto async_accept(io_uring *ring, int fd, sockaddr *addr = nullptr,
                         socklen_t *addrlen = nullptr, int flags = 0) noexcept
{
    return make_uring_awaitable(ring, [=](io_uring_sqe *sqe) {
        io_uring_prep_accept(sqe, fd, addr, addrlen, flags);
    });
}

inline auto async_connect(io_uring *ring, int fd, const sockaddr *addr, socklen_t addrlen) noexcept
{
    return make_uring_awaitable(ring, [=](io_uring_sqe *sqe) {
        io_uring_prep_connect(sqe, fd, addr, addrlen);
    });
}

// 定时器：可用于实现协程版的超时 / sleep。ts 必须存活到 CQE 回来（通常放在协程栈上）。
inline auto async_timeout(io_uring *ring, __kernel_timespec *ts, unsigned count = 0,
                         unsigned flags = 0) noexcept
{
    return make_uring_awaitable(ring, [=](io_uring_sqe *sqe) {
        io_uring_prep_timeout(sqe, ts, count, flags);
    });
}

} // namespace kimrpc::coro
