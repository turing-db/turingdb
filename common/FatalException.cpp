#include "FatalException.h"

FatalException::FatalException()
    : _msg("Fatal exception: logic error.")
{
}

FatalException::FatalException(std::string&& msg)
    : _msg(std::move(msg))
{
}

FatalException::~FatalException() noexcept {
}

const char* FatalException::what() const noexcept {
    return _msg.c_str();
}
