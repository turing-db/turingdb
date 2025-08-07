#pragma once

#include <string>

#include "TuringException.h"

namespace db {

class ASTException : public TuringException {
public:
    ASTException(const ASTException&) = default;
    ASTException(ASTException&&) = default;
    ASTException& operator=(const ASTException&) = default;
    ASTException& operator=(ASTException&&) = default;

    explicit ASTException(std::string&& msg);
    ~ASTException() noexcept override;
};

}
