#pragma once

#include <string>

#include "TuringException.h"

namespace db {

class ProcedureException : public TuringException {
public:
    explicit ProcedureException(std::string&& msg);
    ~ProcedureException() noexcept override;

    ProcedureException(const ProcedureException&) = default;
    ProcedureException(ProcedureException&&) = default;
    ProcedureException& operator=(const ProcedureException&) = default;
    ProcedureException& operator=(ProcedureException&&) = default;
};

}
