#pragma once

#include <string>

#include "TuringException.h"

namespace db {

class AnalyzeException : public TuringException {
public:
    explicit AnalyzeException(std::string&& msg);
    ~AnalyzeException() noexcept override;
};

}
