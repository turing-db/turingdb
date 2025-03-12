#include "PlannerException.h"

using namespace db;

PlannerException::PlannerException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

PlannerException::~PlannerException() {
}
