#pragma once

#include <string>

#include "CompilerException.h"

namespace db {

class AnalyzeException : public CompilerException {
public:
    AnalyzeException(const AnalyzeException&) = default;
    AnalyzeException(AnalyzeException&&) = default;
    AnalyzeException& operator=(const AnalyzeException&) = default;
    AnalyzeException& operator=(AnalyzeException&&) = default;

    explicit AnalyzeException(std::string&& msg);
    ~AnalyzeException() noexcept override;
};

}
