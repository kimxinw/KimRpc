#当前目录下所有源文件放入SRC_LIST
aux_source_directory(. SRC_LIST)

add_library(KimRpc ${SRC_LIST})

find_path(URING_INCLUDE_DIR liburing.h REQUIRED)
find_library(URING_LIB uring REQUIRED)
target_include_directories(KimRpc PUBLIC ${URING_INCLUDE_DIR})

# 服务发现改用 etcd（HTTP/JSON v3 网关），通过 libcurl 访问，替换原 zookeeper_mt
find_package(CURL REQUIRED)
target_include_directories(KimRpc PUBLIC ${CURL_INCLUDE_DIRS})

# KimRpc依赖于muduo
target_link_libraries(KimRpc  muduo_net muduo_base pthread ${CURL_LIBRARIES} ${URING_LIB})
