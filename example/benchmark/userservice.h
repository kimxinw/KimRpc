#pragma once
#include <string>
#include "user.pb.h"
#include<thread>

/**
 * UserService — 压测专用版本
 * 去掉所有 std::cout，避免 I/O 拖慢测量结果
 */
class UserService : public KimRpc::UserServiceRpc
{
public:
    // -------- 本地业务逻辑 --------
    bool Login(const std::string &name, const std::string &pwd)
    {
        std::this_thread::sleep_for(std::chrono::microseconds(500));
        return true;  
    }

    bool Register(uint32_t id, const std::string &name, const std::string &pwd)
    {
        return true;
    }

    // -------- RPC 入口（框架调用） --------
    void Login(::google::protobuf::RpcController *controller,
               const ::KimRpc::LoginRequest        *request,
               ::KimRpc::LoginResponse             *response,
               ::google::protobuf::Closure        *done) override
    {
        bool ok = Login(request->name(), request->pwd());
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        response->set_success(ok);
        done->Run();
    }

    void Register(::google::protobuf::RpcController *controller,
                  const ::KimRpc::RegisterRequest     *request,
                  ::KimRpc::RegisterResponse          *response,
                  ::google::protobuf::Closure        *done) override
    {
        bool ok = Register(request->id(), request->name(), request->pwd());
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        response->set_success(ok);
        done->Run();
    }
};