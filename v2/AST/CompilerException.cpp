#include "CompilerException.h"

using namespace db::v2;

CompilerException::CompilerException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

CompilerException::~CompilerException() noexcept {
}

