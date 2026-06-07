# KimRpc

基于 **C++20 + Protobuf** 的高性能 RPC 框架。服务端提供 **io_uring** 全异步 proactor 实现，请求用 **C++20 协程**派发；服务注册与发现走 **etcd**，客户端内置**多实例负载均衡**。

---

## ✨ 特性

- **io_uring 服务端**：单 proactor + provided buffer ring（连接共享内核缓冲，空闲连接零缓冲占用），accept/recv/send 全异步。
- **协程派发**：每个请求一个协程（`DetachedTask`），业务 `CallMethod` offload 到线程池，完成后经 eventfd 唤回 reactor 线程发送响应。
- **连接级背压**：单连接在途请求达上限即暂停 recv，靠 TCP 窗口回压客户端；并设有 header/body/连接数协议上限。
- **请求多路复用**：响应帧带 `request_id`，同一连接可乱序并发多个请求；`connId` 防 fd 复用串话。
- **etcd 服务发现 + 负载均衡**：provider 用 `lease + keepalive` 注册（进程挂了实例自动下线），client 用 `prefix Range 播种 + Watch 增量` 维护实例表，按**轮询(round-robin)** 选实例。
- **异步客户端**：可选的非阻塞调用路径（回调 + 后台收线程 + `request_id` 解复用 + 定时器超时）。
- **协程基建**：`Task<T>`（惰性 + 对称转移）、`DetachedTask`、io_uring awaiter、`sync_wait`。

---

## 🏗️ 架构

```
TODO
```

- 服务端两套实现：`UringRpcProvider`（io_uring，推荐）/ `RpcProvider`（muduo TcpServer）。
- 客户端：`KimRpcChannel`（持久连接 + 多路复用，含可选异步路径）/ `UringChannel`。

### Wire 协议

```
请求帧:  [4B header_size][RpcHeader(service,method,args_size,request_id)][args]
响应帧:  [4B size][8B request_id][body]          // size = 8 + len(body)
```

---

## 📦 依赖

| 依赖 | 说明 |
|---|---|
| C++20 编译器 | 协程支持（GCC 12+ 更佳，GCC 11 见 `coroutine/task.h` 顶部说明） |
| CMake ≥ 3.10 | 构建系统 |
| Protobuf (protoc 3.12.4) | 业务 IDL + 内部 RpcHeader |
| liburing | io_uring 服务端 |
| libcurl | 访问 etcd v3 HTTP/JSON 网关 |
| muduo | `RpcProvider` 网络层 |
| etcd (服务端) | 服务注册与发现，默认 `127.0.0.1:2379` |

```bash
sudo apt install libcurl4-openssl-dev   # 其余依赖按需安装；etcd 单二进制即可
```

---

## 🔧 构建

```bash
bash autobuild.sh        # cmake + make，产物在 bin/ 与 lib/
# 或手动：
mkdir build && cd build && cmake .. && make -j
```

---

## 📊 Benchmark

`UserService::Login`，单机开发环境粗测，仅供量级参考。

| 并发线程 | Channel 数 | 总请求 | 成功 | 失败 | 耗时 | QPS | 平均延迟 | P50 | P95 | P99 | Max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 1 | 80000 | 80000 | 0 | 14.81 s | 5401 req/s | 1475 us | 1556 us | 2023 us | 2324 us | 74130 us |
| 8 | 2 | 80000 | 80000 | 0 | 18.55 s | 4311 req/s | 1845 us | 1881 us | 2382 us | 2872 us | 78794 us |
| 8 | 4 | 80000 | 80000 | 0 | 17.94 s | 4459 req/s | 1781 us | 1903 us | 2142 us | 2406 us | 76979 us |
| 8 | 6 | 80000 | 80000 | 0 | 19.29 s | 4146 req/s | 1485 us | 1350 us | 2080 us | 2194 us | 79076 us |
| 8 | 8 | 80000 | 80000 | 0 | 11.75 s | 6810 req/s | 1166 us | 1246 us | 1400 us | 1609 us | 77894 us |

> 说明：`bench_user` 开启异步客户端后，每线程发起 10000 次请求，并通过 `-c` 调整 Channel 数。整体 QPS 会受到客户端 Channel 数、连接复用、etcd 发现路径、机器负载和单机环境影响；以上结果用于观察量级和趋势，不代表服务端极限吞吐。

## ⚙️ 配置

`bin/test.conf`（`key=value`，`#` 注释）：

```ini
rpcserverip=127.0.0.1
rpcserverport=8000
etcdip=127.0.0.1
etcdport=2379
```

---

## 📁 目录结构

```
src/                       框架实现
  ├── uringrpcprovider.*   io_uring 服务端
  ├── provider.*           muduo 服务端
  ├── channel.*            客户端 channel（含异步路径）
  ├── etcdclient.*         etcd v3 HTTP/JSON 客户端
  ├── rpcaddresscache.cc   服务发现 + 轮询负载均衡
  ├── include/coroutine/   协程模块：Task / awaiter / sync_wait
  └── include/third_party/ vendored nlohmann/json
example/                   callee / caller / benchmark 示例
```

---
