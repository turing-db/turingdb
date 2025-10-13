#include "ParserException.h"

using namespace db::v2;

ParserException::ParserException(std::string&& msg)
    : ASTException(std::move(msg))
{
}

ParserException::~ParserException() noexcept {
}

