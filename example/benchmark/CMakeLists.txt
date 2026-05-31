cmake_minimum_required(VERSION 3.10)

# ============================================================
#  公共 proto 源文件路径（相对于本目录）
# ============================================================
set(USER_PB  ${CMAKE_CURRENT_SOURCE_DIR}/../user.pb.cc)

# ============================================================
#  1. muduo_provider  —  基于 Muduo 的服务端
# ============================================================
add_executable(muduo_provider
    muduo_provider_main.cc
    ${USER_PB}
)
target_link_libraries(muduo_provider KimRpc protobuf)

# ============================================================
#  2. uring_provider  —  基于 io_uring 的服务端
# ============================================================
add_executable(uring_provider
    uring_provider_main.cc
    ${USER_PB}
)
target_link_libraries(uring_provider KimRpc protobuf)

# ============================================================
#  3. bench_user  —  压测客户端（UserService::Login）
# ============================================================
add_executable(bench_user
    bench_user.cc
    ${USER_PB}
)
target_link_libraries(bench_user KimRpc protobuf)