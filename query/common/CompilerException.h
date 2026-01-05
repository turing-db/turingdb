#pragma once

#include <string>

#include "TuringException.h"

namespace db::v2 {

class CompilerException : public TuringException {
public:
    CompilerException(const CompilerException&) = default;
    CompilerException(CompilerException&&) = default;
    CompilerException& operator=(const CompilerException&) = default;
    CompilerException& operator=(CompilerException&&) = default;

    explicit CompilerException(std::string&& msg);
    ~CompilerException() noexcept override;
};

}
