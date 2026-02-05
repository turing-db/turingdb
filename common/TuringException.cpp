#include "TuringException.h"

TuringException::TuringException()
    : _msg("Something went wrong in Turing, stopping here")
{
}

TuringException::TuringException(std::string&& msg)
    : _msg(std::move(msg))
{
}

TuringException::~TuringException() noexcept {
}

const char* TuringException::what() const noexcept {
    return _msg.c_str();
}
