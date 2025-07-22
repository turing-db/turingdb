#include "ASTException.h"

using namespace db;

ASTException::ASTException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

ASTException::~ASTException() noexcept {
}

