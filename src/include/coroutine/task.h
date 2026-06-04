#pragma once

// KimRpc 协程模块 —— Task<T>
//
// Task<T> 是一个“惰性 + 可被 co_await”的协程返回类型：
//   - 惰性 (initial_suspend = suspend_always)：创建后不立即执行，被 co_await 或
//     被 sync_wait 启动时才运行。
//   - 对称转移 (symmetric transfer)：被 co_await 时直接把控制权交给子协程，
//     子协程结束时再无栈跳回父协程，全程不递归、不爆栈。
//   - co_await task 取回 T（void 特化返回 void），子协程抛出的异常会在父协程重抛。
//
// 适合表达“一次有返回值的异步调用链”，例如服务端发起的下游 RPC。
//
// 如果需要一个“根协程”（没有父协程来 co_await、即发即忘，例如“处理一条连接的
// 一个请求”），用 DetachedTask（见文件末尾）。

#include <coroutine>
#include <cstdio>
#include <exception>
#include <utility>

namespace kimrpc::coro
{

template <typename T>
class Task;

namespace detail
{

// 子协程结束时执行：把控制权对称转移回正在等待它的父协程
struct FinalAwaiter
{
    bool await_ready() const noexcept { return false; }

    template <typename Promise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> self) noexcept
    {
        auto &promise = self.promise();
        if (promise.continuation)
        {
            return promise.continuation; // 跳回父协程
        }
        return std::noop_coroutine();    // 没有父协程（被 sync_wait 直接驱动）
    }

    void await_resume() noexcept {}
};

template <typename T>
struct TaskPromiseBase
{
    std::coroutine_handle<> continuation = nullptr; // 等待本协程结果的父协程
    std::exception_ptr exception = nullptr;

    std::suspend_always initial_suspend() noexcept { return {}; }
    FinalAwaiter final_suspend() noexcept { return {}; }
    void unhandled_exception() noexcept { exception = std::current_exception(); }
};

template <typename T>
struct TaskPromise final : TaskPromiseBase<T>
{
    alignas(T) unsigned char storage[sizeof(T)];
    bool has_value = false;

    Task<T> get_return_object() noexcept;

    template <typename U = T>
    void return_value(U &&value)
    {
        ::new (static_cast<void *>(storage)) T(std::forward<U>(value));
        has_value = true;
    }

    T &result() &
    {
        if (this->exception)
            std::rethrow_exception(this->exception);
        return *reinterpret_cast<T *>(storage);
    }

    T &&result() &&
    {
        if (this->exception)
            std::rethrow_exception(this->exception);
        return std::move(*reinterpret_cast<T *>(storage));
    }

    ~TaskPromise()
    {
        if (has_value)
            reinterpret_cast<T *>(storage)->~T();
    }
};

template <>
struct TaskPromise<void> final : TaskPromiseBase<void>
{
    Task<void> get_return_object() noexcept;
    void return_void() noexcept {}
    void result()
    {
        if (exception)
            std::rethrow_exception(exception);
    }
};

} // namespace detail

template <typename T = void>
class [[nodiscard]] Task
{
public:
    using promise_type = detail::TaskPromise<T>;
    using handle_type = std::coroutine_handle<promise_type>;

    Task() noexcept = default;
    explicit Task(handle_type h) noexcept : m_handle(h) {}

    Task(Task &&other) noexcept : m_handle(std::exchange(other.m_handle, {})) {}
    Task &operator=(Task &&other) noexcept
    {
        if (this != &other)
        {
            if (m_handle)
                m_handle.destroy();
            m_handle = std::exchange(other.m_handle, {});
        }
        return *this;
    }

    Task(const Task &) = delete;
    Task &operator=(const Task &) = delete;

    ~Task()
    {
        if (m_handle)
            m_handle.destroy();
    }

    // co_await task：挂起父协程，启动本协程（对称转移），本协程结束后唤回父协程并取回结果
    auto operator co_await() const &noexcept
    {
        struct Awaiter
        {
            handle_type coro;
            bool await_ready() const noexcept { return !coro || coro.done(); }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept
            {
                coro.promise().continuation = awaiting;
                return coro; // 对称转移：直接开始运行子协程
            }
            decltype(auto) await_resume() { return coro.promise().result(); }
        };
        return Awaiter{m_handle};
    }

    auto operator co_await() const &&noexcept
    {
        struct Awaiter
        {
            handle_type coro;
            bool await_ready() const noexcept { return !coro || coro.done(); }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept
            {
                coro.promise().continuation = awaiting;
                return coro;
            }
            decltype(auto) await_resume() { return std::move(coro.promise()).result(); }
        };
        return Awaiter{m_handle};
    }

    handle_type handle() const noexcept { return m_handle; }
    bool done() const noexcept { return !m_handle || m_handle.done(); }

private:
    handle_type m_handle{};
};

namespace detail
{
template <typename T>
inline Task<T> TaskPromise<T>::get_return_object() noexcept
{
    return Task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(*this)};
}
inline Task<void> TaskPromise<void>::get_return_object() noexcept
{
    return Task<void>{std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
}
} // namespace detail

// 即发即忘的根协程：调用即开始执行（initial_suspend = never），运行到第一个挂起点
// （通常是 co_await 某个 IO）后让出；整个协程完成时自动销毁帧（final_suspend = never），
// 无需调用方持有/回收。适合“处理一条请求”这种入口。
//
//   DetachedTask handleRequest(io_uring* ring, int fd) {
//       co_await ...;          // 各种异步 IO
//   }                          // 完成后帧自动释放
//
// 注意：根协程不应让异常逃逸；这里会捕获并打到 stderr（帧已无父协程可重抛）。
struct DetachedTask
{
    struct promise_type
    {
        DetachedTask get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept
        {
            try
            {
                std::rethrow_exception(std::current_exception());
            }
            catch (const std::exception &e)
            {
                std::fprintf(stderr, "[coro] unhandled exception in DetachedTask: %s\n", e.what());
            }
            catch (...)
            {
                std::fprintf(stderr, "[coro] unhandled non-std exception in DetachedTask\n");
            }
        }
    };
};

} // namespace kimrpc::coro
