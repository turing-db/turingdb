#include "TuringDBException.h"

TuringDBException::TuringDBException()
    : _msg("Something went wrong in TuringDB, stopping here")
{
}

TuringDBException::TuringDBException(std::string&& msg)
    : _msg(std::move(msg))
{
}

const char* TuringDBException::what() const noexcept {
    return _msg.c_str();
}
