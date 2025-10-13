#pragma once

#include <string>

#include "ASTException.h"

namespace db::v2 {

class AnalyzeException : public ASTException {
public:
    AnalyzeException(const AnalyzeException&) = default;
    AnalyzeException(AnalyzeException&&) = default;
    AnalyzeException& operator=(const AnalyzeException&) = default;
    AnalyzeException& operator=(AnalyzeException&&) = default;

    explicit AnalyzeException(std::string&& msg);
    ~AnalyzeException() noexcept override;
};

}
