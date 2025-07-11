#include "ParserException.h"

using namespace db;

ParserException::ParserException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

ParserException::~ParserException() noexcept {
}

