#include "uringchannel.h"

#include "controller.h"
#include "rpcaddresscache.h"
#include "rpcheader.pb.h"
#include "uringtcpclient.h"

#include <cstdio>
#include <iostream>
#include <string>

/**
 * header_size + service_name method_name args_size args
 */
void KimRpcUringChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                                   google::protobuf::RpcController *controller,
                                   const google::protobuf::Message *request,
                                   google::protobuf::Message *response,
                                   google::protobuf::Closure *done)
{
    const google::protobuf::ServiceDescriptor *sd = method->service();
    std::string service_name = sd->name();
    std::string method_name = method->name();

    uint32_t args_size = 0;
    std::string args_str;
    if (request->SerializeToString(&args_str))
    {
        args_size = args_str.size();
    }
    else
    {
        controller->SetFailed("serialize request error!");
        return;
    }

    KimRpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);

    uint32_t header_size = 0;
    std::string rpc_header_str;
    if (rpcHeader.SerializeToString(&rpc_header_str))
    {
        header_size = rpc_header_str.size();
    }
    else
    {
        controller->SetFailed("serialize rpcHeader error!");
        return;
    }

    std::string send_rpc_str;
    send_rpc_str.insert(0, std::string((char *)&header_size, 4));
    send_rpc_str += rpc_header_str;
    send_rpc_str += args_str;

    std::cout << "==================================" << std::endl;
    std::cout << "header_size:" << header_size << std::endl;
    std::cout << "rpc_header_str:" << rpc_header_str << std::endl;
    std::cout << "service_name:" << service_name << std::endl;
    std::cout << "method_name:" << method_name << std::endl;
    std::cout << "args_str:" << args_str << std::endl;
    std::cout << "args_size:" << args_size << std::endl;
    std::cout << "==================================" << std::endl;

    std::string method_path = "/" + service_name + "/" + method_name;
    std::string host_data;
    std::string error;
    if (!GetRpcAddress(method_path, &host_data, &error))
    {
        controller->SetFailed(error);
        return;
    }

    int idx = host_data.find(":");
    if (idx == -1)
    {
        controller->SetFailed(method_name + " address is invalid");
        return;
    }
    std::string ip = host_data.substr(0, idx);
    uint16_t port = stoi(host_data.substr(idx + 1, host_data.size() - idx));

    UringTcpClient client;
    if (!client.Connect(ip, port, &error))
    {
        controller->SetFailed(error);
        return;
    }

    if (!client.WriteAll(send_rpc_str.c_str(), send_rpc_str.size(), &error))
    {
        controller->SetFailed(error);
        return;
    }

    uint32_t response_size = 0;
    if (!client.ReadExact((char *)&response_size, sizeof(uint32_t), &error))
    {
        controller->SetFailed(error);
        return;
    }

    std::string response_str;
    response_str.resize(response_size);
    if (response_size > 0 && !client.ReadExact(&response_str[0], response_size, &error))
    {
        controller->SetFailed(error);
        return;
    }

    if (!response->ParseFromArray(response_str.data(), response_str.size()))
    {
        char errtxt[2048] = {0};
        snprintf(errtxt, sizeof(errtxt), "parse error! response_str:%s", response_str.c_str());
        controller->SetFailed(errtxt);
        return;
    }

    if (done != nullptr)
    {
        done->Run();
    }
}
