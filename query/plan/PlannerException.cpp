#include "PlannerException.h"

using namespace db;

PlannerException::PlannerException(std::string&& msg)
    : CompilerException(std::move(msg))
{
}

PlannerException::~PlannerException() noexcept {
}
