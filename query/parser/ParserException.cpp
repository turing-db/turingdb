#include "ParserException.h"

using namespace db;

ParserException::ParserException(std::string&& msg)
    : CompilerException(std::move(msg))
{
}

ParserException::~ParserException() noexcept {
}

