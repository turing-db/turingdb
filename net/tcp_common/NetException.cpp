#include "NetException.h"

#include "spdlog/fmt/fmt.h"

NetException::NetException()
    : TuringException(fmt::format("Net error: {}", strerror(errno)))
{
}

NetException::NetException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

NetException::~NetException() noexcept {
}

const char* NetException::what() const noexcept {
    return _msg.c_str();
}
