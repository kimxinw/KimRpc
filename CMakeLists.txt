cmake_minimum_required(VERSION 3.0)

project(KimRpc)

#生成可调试的debug版本
set(CMAKE_BUILD_TYPE "Debug")

# 协程模块需要 C++20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#设置项目可执行文件输出路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)

#设置项目库文件输出路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

#设置头文件搜索路径  -I
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/src/include)
include_directories(${PROJECT_SOURCE_DIR}/example)

# 设置库文件搜索路径  -L
link_directories(${PROJECT_SOURCE_DIR}/lib)

#src包含了KimRpc框架所有的相关代码
add_subdirectory(src)
#example包含了KimRpc框架使用的示例代码
add_subdirectory(example)