#include "ASTException.h"

using namespace db::v2;

ASTException::ASTException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

ASTException::~ASTException() noexcept {
}

