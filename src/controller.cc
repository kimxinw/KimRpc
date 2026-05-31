#include "controller.h"

KimRpcController::KimRpcController()
    : m_failed(false),
      m_errText("")
{
}

void KimRpcController::Reset()
{
    m_failed = false;
    m_errText = "";
}

bool KimRpcController::Failed() const
{
    return m_failed;
}

std::string KimRpcController::ErrorText() const
{
    return m_errText;
}

void KimRpcController::SetFailed(const std::string &reason)
{
    m_failed = true;
    m_errText = reason;
}

void KimRpcController::StartCancel()
{
}

bool KimRpcController::IsCanceled() const
{
    return false;
}

void KimRpcController::NotifyOnCancel(google::protobuf::Closure *callback)
{
}
