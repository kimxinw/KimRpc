# set(SRC_LIST userservice.cc ../user.pb.cc)
set(SRC_LIST friendservice.cc ../friend.pb.cc)

add_executable(provider ${SRC_LIST})

#需要链接自己实现的KimRpc和google的protobuf
target_link_libraries(provider KimRpc protobuf)