#include "CountStep.h"

#include <sstream>

using namespace db;

void CountStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "CountStep";
    ss << " input=" << std::hex << _input;
    ss << " count=" << std::hex << _count;
    descr = ss.str();
}
