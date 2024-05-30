#include "Panic.h"


TuringException::TuringException(std::string&& msg)
    : _msg(std::move(msg))
{
}

const char* TuringException::what() const noexcept {
    return _msg.c_str();
}

void panic() {
    throw TuringException();
}

void panic(std::string&& msg) {
    throw TuringException(std::move(msg));
}

