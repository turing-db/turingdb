#pragma once

#include <string>

#include "CompilerException.h"

namespace db {

class IRException : public CompilerException {
public:
    IRException(const IRException&) = default;
    IRException(IRException&&) = default;
    IRException& operator=(const IRException&) = default;
    IRException& operator=(IRException&&) = default;

    explicit IRException(std::string&& msg);
    ~IRException() noexcept override;
};

}
