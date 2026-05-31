#include "connectionmanager.h"

ConnectionContext *ConnectionManager::acquire(int fd)
{
    ConnectionContext *ctx;
    if (!m_idlePool.empty())
    {
        ctx = m_idlePool.back();
        m_idlePool.pop_back();
        ctx->reset(fd);
    }
    else
    {
        auto newCtx = std::make_unique<ConnectionContext>();
        ctx = newCtx.get();
        ctx->reset(fd);
        m_allConns.push_back(std::move(newCtx));
    }
    m_active[fd] = ctx;
    return ctx;
}

ConnectionContext *ConnectionManager::find(int fd)
{
    auto it = m_active.find(fd);
    return it != m_active.end() ? it->second : nullptr;
}

bool ConnectionManager::onComplete(ConnectionContext *ctx){
    if (ctx == nullptr) return false;   // accept / notify 无 ctx
    --ctx->inflight;
    if (ctx->closing && ctx->inflight == 0)
    {
        recycle(ctx);
        return true;   // 已回收，调用方别再碰
    }
    return false;
}

void ConnectionManager::close(int fd)
{
    auto it = m_active.find(fd);
    if (it == m_active.end())
    {
        // if (fd != -1) ::close(fd);
        return;
    }
    ConnectionContext *ctx = it->second;
    m_active.erase(it);
    ::close(ctx->fd);
    ctx->closing = true;
    if (ctx->inflight == 0)
    {
        recycle(ctx);
    }
}

void ConnectionManager::recycle(ConnectionContext *ctx)
{
    m_idlePool.push_back(ctx);
}