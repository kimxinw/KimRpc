#include "rpcaddresscache.h"

#include "zookeeperutil.h"

#include <mutex>
#include <string>
#include <unordered_map>

namespace
{
std::mutex g_addressCacheMutex;
std::unordered_map<std::string, std::string> g_addressCache;
}

bool GetRpcAddress(const std::string &method_path, std::string *host_data, std::string *error)
{
    {
        std::lock_guard<std::mutex> lock(g_addressCacheMutex);
        auto it = g_addressCache.find(method_path);
        if (it != g_addressCache.end())
        {
            *host_data = it->second;
            return true;
        }
    }

    ZkClient zkCli;
    zkCli.Start();
    std::string zk_data = zkCli.GetData(method_path.c_str());
    if (zk_data == "")
    {
        if (error != nullptr)
        {
            *error = method_path + " is not exist!";
        }
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(g_addressCacheMutex);
        g_addressCache[method_path] = zk_data;
    }
    *host_data = zk_data;
    return true;
}
