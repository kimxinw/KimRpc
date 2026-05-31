#include "provider.h"
#include "application.h"
#include "rpcheader.pb.h"
#include "logger.h"
#include "zookeeperutil.h"

#include <cstring>

void RpcProvider::NotifyService(google::protobuf::Service *service)
{
    ServiceInfo service_info;

    // 获取服务对象的描述信息
    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    // 获取服务的名字
    std::string service_name = pserviceDesc->name();
    // 获取服务对象的service方法的数量
    int methodCnt = pserviceDesc->method_count();

    // std::cout << "service_name:" << service_name << std::endl;
    LOG_INFO("service_name:%s", service_name.c_str());

    for (int i = 0; i < methodCnt; i++)
    {
        // 获取了服务对象指定下标的服务方法的描述 (抽象描述)
        const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);

        std::string method_name = pmethodDesc->name();
        service_info.m_methodMap.insert({method_name, pmethodDesc});

        // std::cout << "method_name:" << method_name << std::endl;
        LOG_INFO("method name:%s", method_name.c_str());
    }
    service_info.m_service = service;
    m_serviceMap.insert({service_name, service_info});
}

void RpcProvider::Run()
{
    std::string ip = KimRpcApplication::GetInstance().GetConfig().load("rpcserverip");
    uint16_t port = stoi(KimRpcApplication::GetInstance().GetConfig().load("rpcserverport"));
    muduo::net::InetAddress address(ip, port);

    // 创建TcpServer对象
    muduo::net::TcpServer server(&m_eventLoop, address, "RpcProvider");
    // 绑定连接回调和消息读写回调方法  分离了网络代码和业务代码
    server.setConnectionCallback(std::bind(&RpcProvider::onConnection, this, std::placeholders::_1));
    server.setMessageCallback(std::bind(&RpcProvider::onMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

    // 设置muduo的线程数量
    server.setThreadNum(4);

    // 把当前rpc节点上要发布的服务全部注册到zk上面，让rpc cllient可以从zk上发现服务
    ZkClient zkCli;
    zkCli.Start(); // 连接zkserver
    // servive_name为永久性节点， method_name为临时性节点
    for (auto &sp : m_serviceMap)
    {
        // service_name
        std::string service_path = "/" + sp.first;
        zkCli.Create(service_path.c_str(), nullptr, 0);
        for (auto &mp : sp.second.m_methodMap)
        {
            // service_name/method_name
            std::string method_path = service_path + "/" + mp.first;
            char method_path_data[128] = {0};
            sprintf(method_path_data, "%s:%d", ip.c_str(), port);
            //ZOO_EPHEMERAL表示znode是一个临时性节点
            zkCli.Create(method_path.c_str(), method_path_data, strlen(method_path_data), ZOO_EPHEMERAL);
        }
    }

    // std::cout << "Rpcprovide start service at ip:" << ip << " port:" << port << std::endl;
    LOG_INFO("Rpcprovide start service at ip:%s port:%d", ip.c_str(), port);

    // 启动网络服务
    server.start();
    m_eventLoop.loop();
}

void RpcProvider::onConnection(const muduo::net::TcpConnectionPtr &conn)
{
    // rpc请求时短连接，rpc服务端返回响应结果后关闭连接
    if (!conn->connected())
    {
        // 和rpc client断开连接了
        conn->shutdown(); // 关闭socketfd
    }
}

/**
 * 在框架内部，RpcProvider和RpcConsumer协商好之间通信用的protobuf数据类型
 * service_name method_name args  定义proto的message类型，进行数据头的序列化和反序列化
 * header_size + header_str + args_str
 */
// 如果远程有一个rpc服务的调用请求，onMessage就会响应
void RpcProvider::onMessage(const muduo::net::TcpConnectionPtr &conn,
                            muduo::net::Buffer *buffer,
                            muduo::Timestamp time)
{
    // 循环排空缓冲区里所有完整请求（支持流水线/多路复用：一次可能到达多个请求或半个请求）
    while (buffer->readableBytes() >= sizeof(uint32_t))
    {
        const char *base = buffer->peek();

        uint32_t header_size = 0;
        ::memcpy(&header_size, base, sizeof(uint32_t));

        // 头部还没收全，等下次 onMessage
        if (buffer->readableBytes() < sizeof(uint32_t) + header_size)
        {
            break;
        }

        std::string rpc_header_str(base + sizeof(uint32_t), header_size);
        KimRpc::RpcHeader rpcHeader;
        if (!rpcHeader.ParseFromString(rpc_header_str))
        {
            LOG_ERROR("rpc_header_str:%s parse error!", rpc_header_str.c_str());
            conn->shutdown();
            return;
        }

        std::string service_name = rpcHeader.service_name();
        std::string method_name = rpcHeader.method_name();
        uint32_t args_size = rpcHeader.args_size();
        uint64_t request_id = rpcHeader.request_id();

        size_t frame_size = sizeof(uint32_t) + header_size + args_size;
        // 整个请求帧还没收全，等下次 onMessage
        if (buffer->readableBytes() < frame_size)
        {
            break;
        }

        std::string args_str(base + sizeof(uint32_t) + header_size, args_size);
        buffer->retrieve(frame_size); // 消费掉这一帧

        // 获取service对象和method对象
        auto it = m_serviceMap.find(service_name);
        if (it == m_serviceMap.end())
        {
            LOG_ERROR("%s is not exist!", service_name.c_str());
            continue;
        }
        ServiceInfo &sinfo = it->second;

        auto mit = sinfo.m_methodMap.find(method_name);
        if (mit == sinfo.m_methodMap.end())
        {
            LOG_ERROR("%s:%s is not exist!", service_name.c_str(), method_name.c_str());
            continue;
        }

        google::protobuf::Service *service = sinfo.m_service;
        const google::protobuf::MethodDescriptor *method = mit->second;

        google::protobuf::Message *request = service->GetRequestPrototype(method).New();
        if (!request->ParseFromString(args_str))
        {
            LOG_ERROR("request parse error! content: %s", args_str.c_str());
            delete request;
            continue;
        }
        google::protobuf::Message *response = service->GetResponsePrototype(method).New();

        // 把 conn 和 request_id 打包随 done 传给业务侧（NewCallback 最多绑 2 个参数）
        ResponseTag *tag = new ResponseTag{conn, request_id};
        google::protobuf::Closure *done = google::protobuf::NewCallback<RpcProvider,
                                                                        ResponseTag *,
                                                                        google::protobuf::Message *>(this,
                                                                                                     &RpcProvider::sendRpcResponse,
                                                                                                     tag, response);

        // 在框架上根据远端rpc请求，调用当前rpc节点上发布的方法
        service->CallMethod(method, nullptr, request, response, done);
        delete request;
    }
}

void RpcProvider::sendRpcResponse(ResponseTag *tag, google::protobuf::Message *response)
{
    muduo::net::TcpConnectionPtr conn = tag->conn;
    uint64_t request_id = tag->request_id;
    delete tag;

    std::string response_str;
    if (response->SerializeToString(&response_str)) // response进行序列化
    {
        // wire: [4字节 size][8字节 request_id][body], size = 8 + body
        uint32_t response_size = sizeof(uint64_t) + response_str.size();
        std::string send_buf;
        send_buf.reserve(sizeof(uint32_t) + response_size);
        send_buf.append((char *)&response_size, sizeof(uint32_t));
        send_buf.append((char *)&request_id, sizeof(uint64_t));
        send_buf += response_str;

        // 序列化成功后，通过网络把rpc方法执行的结果发送回rpc的调用方
        conn->send(send_buf);
    }
    else
    {
        // std::cout << "serialize response_str error" << std::endl;
        LOG_ERROR("serialize response_str error");
    }
    delete response;
}
