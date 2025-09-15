#pragma once

#include <string>

#include "TuringException.h"

namespace db::v2 {

class PlannerException : public TuringException {
public:
    explicit PlannerException(std::string&& msg)
        : TuringException(std::move(msg))
    {
    }
};

}
