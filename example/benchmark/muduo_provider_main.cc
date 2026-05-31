#include "application.h"
#include "provider.h"
#include "userservice.h"

int main(int argc, char **argv)
{
    KimRpcApplication::Init(argc, argv);

    RpcProvider provider;
    provider.NotifyService(new UserService());
    provider.Run();

    return 0;
}