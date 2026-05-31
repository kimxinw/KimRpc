#pragma once

#pragma once

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

class KimRpcUringChannel : public google::protobuf::RpcChannel
{
public:
    // 所有通过stub代理对象调用的rpc方法都走到这里，使用io_uring完成网络发送和接收
    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;
};
