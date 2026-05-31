#include <iostream>

#include "application.h"
#include "friend.pb.h"

int main(int argc, char **argv)
{
    KimRpcApplication::Init(argc, argv);

    KimRpc::FriendServiceRpc_Stub stub(new KimRpcChannel());

    // rpc方法的请求参数
    KimRpc::GetFriendListRequest request;
    request.set_userid(1000);

    // rpc方法的响应
    KimRpc::GetFriendListResponse response;

    // 发起rpc调用
    KimRpcController controller;
    stub.GetFriendList(&controller, &request, &response, nullptr);

    // 一次rpc调用完成，读调用的结果
    if (controller.Failed())
    {
        std::cout << controller.ErrorText() << std::endl;
    }
    else
    {
        if (response.result().errcode() == 0)
        {
            std::cout << "rpc GetFriendList response success" << std::endl;
            int size = response.friends_size();
            for (int i = 0; i < size; i++)
            {
                std::cout << "index:" << i + 1 << " name:" << response.friends(i) << std::endl;
            }
        }
        else
        {
            std::cout << "rpc GetFriendList response error:" << response.result().errmsg() << std::endl;
        }
    }
}