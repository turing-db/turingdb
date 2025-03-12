#pragma once

#include <string>

#include "TuringException.h"

namespace db {

class PlannerException : public TuringException {
public:
    explicit PlannerException(std::string&& msg);
    ~PlannerException() noexcept override;
};

}
