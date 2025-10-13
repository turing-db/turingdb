#pragma once

#include <string>

#include "ASTException.h"

namespace db::v2 {

class PlannerException : public ASTException {
public:
    explicit PlannerException(std::string&& msg)
        : ASTException(std::move(msg))
    {
    }
};

}
