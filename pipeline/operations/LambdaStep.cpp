#include "LambdaStep.h"

#include <sstream>

using namespace db;

void LambdaStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "LambdaStep";
    ss << " func=" << std::hex << (uintptr_t)_func.target<void(*)(LambdaStep*, LambdaStep::Operation)>();
    descr = ss.str();
}
