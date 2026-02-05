#pragma once

#include <string>

#include "CompilerException.h"

namespace db {

class PlannerException : public CompilerException {
public:
    explicit PlannerException(std::string&& msg);
    ~PlannerException() noexcept override;
};

}
