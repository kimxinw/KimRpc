#include <iostream>

#include "application.h"
#include "user.pb.h"
#include "channel.h"

#ifdef KIMRPC_ASYNC_CLIENT
#include <future>
#include <google/protobuf/stubs/callback.h>
#include "controller.h"

// 一次异步调用所需的全部状态：response/controller/回调闭包必须活到回调触发，
// 故统一堆分配，由回调里 set_value 通知主线程后再回收
struct AsyncLogin
{
    KimRpc::LoginResponse response;
    KimRpcController controller;
    std::promise<void> done;

    void onDone()
    {
        if (controller.Failed())
            std::cout << "[async] login rpc failed: " << controller.ErrorText() << std::endl;
        else if (response.result().errcode() != 0)
            std::cout << "[async] login biz error: " << response.result().errmsg() << std::endl;
        else
            std::cout << "[async] login success: " << response.success() << std::endl;
        done.set_value();
    }
};

int main(int argc, char **argv)
{
    KimRpcApplication::Init(argc, argv);

    KimRpc::UserServiceRpc_Stub stub(new KimRpcChannel());

    KimRpc::LoginRequest request;
    request.set_name("zhang san");
    request.set_pwd("123456");

    // 非阻塞：CallMethod 立即返回，响应到达时收线程触发 onDone。
    // 这里连发两次，二者并发在途，再统一等待
    auto *c1 = new AsyncLogin();
    auto *c2 = new AsyncLogin();
    auto f1 = c1->done.get_future();
    auto f2 = c2->done.get_future();

    stub.Login(&c1->controller, &request, &c1->response,
               google::protobuf::NewCallback(c1, &AsyncLogin::onDone));
    stub.Login(&c2->controller, &request, &c2->response,
               google::protobuf::NewCallback(c2, &AsyncLogin::onDone));
    std::cout << "[async] both Login dispatched, not blocked" << std::endl;

    f1.wait();
    f2.wait();
    delete c1;
    delete c2;
    return 0;
}
#else
int main(int argc, char **argv)
{
    // 整个服务启动以后，想使用KimRpc框架来享受rpc服务调用，一定要先调用框架的初始化函数
    // 框架Init是静态方法，只初始化一次
    KimRpcApplication::Init(argc, argv);

    // 演示调用远程发布的rpc方法Login
    KimRpc::UserServiceRpc_Stub stub(new KimRpcChannel());
    // rpc方法的请求参数
    KimRpc::LoginRequest request;
    request.set_name("zhang san");
    request.set_pwd("123456");
    // rpc方法的响应
    KimRpc::LoginResponse response;
    // 发起rpc方法的调用  同步的rpc调用过程  KimRpc::callMethod
    stub.Login(nullptr, &request, &response, nullptr); // Login()底层调用RpcChannel的callMethod，callMethod集中做所有rpc方法调用的参数序列化和网络发送

    // 一次rpc调用完成，读调用的结果
    if (response.result().errcode() == 0)
    {
        std::cout << "rpc login response success:" << response.success() << std::endl;
    }
    else
    {
        std::cout << "rpc login response error:" << response.result().errmsg() << std::endl;
    }

    // 演示调用远程发布的rpc方法Register
    KimRpc::RegisterRequest req;
    req.set_id(2000);
    req.set_name("li si");
    req.set_pwd("12345");

    KimRpc::RegisterResponse rsp;
    stub.Register(nullptr, &req, &rsp, nullptr);

    if (rsp.result().errcode() == 0)
    {
        std::cout << "rpc login response success:" << rsp.success() << std::endl;
    }
    else
    {
        std::cout << "rpc login response error:" << rsp.result().errmsg() << std::endl;
    }

    return 0;
}
#endif // KIMRPC_ASYNC_CLIENT