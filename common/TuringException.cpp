#include "TuringException.h"

TuringException::TuringException()
    : _msg("Something went wrong in the Turing platform, stopping here")
{
}

TuringException::TuringException(std::string&& msg)
    : _msg(std::move(msg))
{
}

const char* TuringException::what() const noexcept {
    return _msg.c_str();
}
