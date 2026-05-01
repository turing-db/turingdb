#include "ProcedureException.h"

using namespace db;

ProcedureException::ProcedureException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

ProcedureException::~ProcedureException() noexcept {
}
