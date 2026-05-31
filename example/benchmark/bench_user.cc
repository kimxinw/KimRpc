#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <thread>
#include <memory>
#include <mutex>
#include <atomic>
#include <numeric>
#include <algorithm>
#include <chrono>
#include <zookeeper/zookeeper.h>

#include "application.h"
#include "channel.h"
#include "controller.h"
#include "user.pb.h"

// ============================================================
//  全局统计
// ============================================================
static std::atomic<int>      g_success{0};
static std::atomic<int>      g_failed{0};
static std::vector<long long> g_latencies;   // 微秒
static std::mutex             g_lat_mutex;

// ============================================================
//  单个工作线程
// ============================================================
static void worker(int thread_id, int n_requests, KimRpcChannel *channel)
{
    // channel 由连接池分配，多个线程可能共享同一条（多路复用）
    KimRpc::UserServiceRpc_Stub stub(channel);

    std::vector<long long> local_lat;
    local_lat.reserve(n_requests);

    for (int i = 0; i < n_requests; ++i)
    {
        KimRpc::LoginRequest  req;
        KimRpc::LoginResponse rsp;
        req.set_name("bench_" + std::to_string(thread_id));
        req.set_pwd("password123");

        KimRpcController ctrl;

        auto t0 = std::chrono::high_resolution_clock::now();
        stub.Login(&ctrl, &req, &rsp, nullptr);
        auto t1 = std::chrono::high_resolution_clock::now();

        long long us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        local_lat.push_back(us);

        if (ctrl.Failed() || rsp.result().errcode() != 0)
            ++g_failed;
        else
            ++g_success;
    }

    std::lock_guard<std::mutex> lk(g_lat_mutex);
    g_latencies.insert(g_latencies.end(), local_lat.begin(), local_lat.end());
}

// ============================================================
//  分位数辅助
// ============================================================
static long long pct(const std::vector<long long> &v, double p)
{
    if (v.empty()) return 0;
    int idx = std::min((int)(v.size() * p), (int)v.size() - 1);
    return v[idx];
}

// ============================================================
//  main
// ============================================================
int main(int argc, char **argv)
{
    int threads   = 1;
    int n_per_thr = 1000;
    int channels_n = 1;   // 连接池大小：线程按 i % channels_n 分配到这些连接上

    // 先摘出 -t / -n / -c，剩余参数重新打包给 KimRpcApplication::Init
    std::vector<char *> KimRpc_argv;
    KimRpc_argv.push_back(argv[0]);

    for (int i = 1; i < argc; ++i)
    {
        std::string a = argv[i];
        if      (a == "-t" && i + 1 < argc) threads    = std::stoi(argv[++i]);
        else if (a == "-n" && i + 1 < argc) n_per_thr  = std::stoi(argv[++i]);
        else if (a == "-c" && i + 1 < argc) channels_n = std::stoi(argv[++i]);
        else    KimRpc_argv.push_back(argv[i]);   // -i <config> 等原样保留
    }
    if (channels_n < 1) channels_n = 1;

    int KimRpc_argc = (int)KimRpc_argv.size();
    KimRpcApplication::Init(KimRpc_argc, KimRpc_argv.data());
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);

    const int total = threads * n_per_thr;

    std::cout << "\n";
    std::cout << "┌─────────────────────────────────┐\n";
    std::cout << "│   Bench  UserService :: Login    │\n";
    std::cout << "├──────────────┬──────────────────┤\n";
    std::cout << "│ Threads      │ " << std::setw(16) << threads    << " │\n";
    std::cout << "│ Channels     │ " << std::setw(16) << channels_n << " │\n";
    std::cout << "│ Req/thread   │ " << std::setw(16) << n_per_thr  << " │\n";
    std::cout << "│ Total req    │ " << std::setw(16) << total      << " │\n";
    std::cout << "└──────────────┴──────────────────┘\n";
    std::cout << "Running...\n\n";

    // -------- 启动压测 --------
    auto wall0 = std::chrono::high_resolution_clock::now();

    // 连接池：channels_n 条连接，线程轮询分配，必须活到 join 之后
    std::vector<std::unique_ptr<KimRpcChannel>> channels;
    channels.reserve(channels_n);
    for (int i = 0; i < channels_n; ++i)
        channels.push_back(std::make_unique<KimRpcChannel>());

    std::vector<std::thread> pool;
    pool.reserve(threads);
    for (int i = 0; i < threads; ++i)
        pool.emplace_back(worker, i, n_per_thr, channels[i % channels_n].get());
    for (auto &t : pool)
        t.join();

    auto wall1   = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(wall1 - wall0).count();

    // -------- 汇总 --------
    std::sort(g_latencies.begin(), g_latencies.end());
    int  sz  = (int)g_latencies.size();
    double avg = sz ? (double)std::accumulate(g_latencies.begin(),
                                              g_latencies.end(), 0LL) / sz : 0;
    double qps = elapsed > 0 ? g_success.load() / elapsed : 0;

    // -------- 打印结果 --------
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "┌──────────────┬──────────────────┐\n";
    std::cout << "│ Result       │            Value  │\n";
    std::cout << "├──────────────┼──────────────────┤\n";
    std::cout << "│ Total        │ " << std::setw(14) << total           << " req │\n";
    std::cout << "│ Success      │ " << std::setw(14) << g_success       << " req │\n";
    std::cout << "│ Failed       │ " << std::setw(14) << g_failed        << " req │\n";
    std::cout << "│ Time         │ " << std::setw(13) << elapsed         << " s   │\n";
    std::cout << "│ QPS          │ " << std::setw(12) << (long long)qps  << " req/s│\n";
    std::cout << "├──────────────┼──────────────────┤\n";
    std::cout << "│ Avg latency  │ " << std::setw(13) << (long long)avg  << " us  │\n";
    std::cout << "│ P50          │ " << std::setw(13) << pct(g_latencies,0.50) << " us  │\n";
    std::cout << "│ P95          │ " << std::setw(13) << pct(g_latencies,0.95) << " us  │\n";
    std::cout << "│ P99          │ " << std::setw(13) << pct(g_latencies,0.99) << " us  │\n";
    std::cout << "│ Max          │ " << std::setw(13) << (sz ? g_latencies.back() : 0) << " us  │\n";
    std::cout << "└──────────────┴──────────────────┘\n\n";

    return 0;
}