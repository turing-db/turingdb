#pragma once

#include <string>

#include "TuringException.h"

namespace db {

class PlannerException : public TuringException {
public:
    explicit PlannerException(std::string&& msg)
        : _msg(std::move(msg))
    {
    }

private:
    std::string _msg;
};

}
