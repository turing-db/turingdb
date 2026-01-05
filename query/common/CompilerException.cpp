#include "CompilerException.h"

using namespace db;

CompilerException::CompilerException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

CompilerException::~CompilerException() noexcept {
}

