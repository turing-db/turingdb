#include "IRException.h"

using namespace db;

IRException::IRException(std::string&& msg)
    : CompilerException(std::move(msg))
{
}

IRException::~IRException() noexcept {
}
