#pragma once

#include "config.h"

#include "channel.h"
#include "controller.h"
#include "rpcheader.pb.h"
#include "provider.h"

// KimRpc框架的基础类，负责框架的一些初始化操作  只需要一个基础类，单例
class KimRpcApplication
{
public:
    static void Init(int argc, char **argv);
    static KimRpcApplication &GetInstance();
    static KimRpcConfig& GetConfig();

private:
    static KimRpcConfig m_config;

    KimRpcApplication() {}
    KimRpcApplication(const KimRpcApplication &) = delete;
    KimRpcApplication(KimRpcApplication &&) = delete;
};