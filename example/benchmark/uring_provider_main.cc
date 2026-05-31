#include "application.h"
#include "uringrpcprovider.h"
#include "userservice.h"

int main(int argc, char **argv)
{
    KimRpcApplication::Init(argc, argv);

    UringRpcProvider provider;
    provider.NotifyService(new UserService());
    provider.Run();

    return 0;
}