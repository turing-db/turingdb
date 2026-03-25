#include "ProtocolException.h"

ProtocolException::ProtocolException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

ProtocolException::~ProtocolException() noexcept {
}

const char* ProtocolException::what() const noexcept {
    return _msg.c_str();
}
