#include "ParserException.h"

using namespace db::v2;

ParserException::ParserException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

ParserException::~ParserException() noexcept {
}

