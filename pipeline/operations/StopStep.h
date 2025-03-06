#pragma once

#include <string>

namespace db {

class ExecutionContext;

class StopStep {
public:
    struct Tag {};

    void prepare(ExecutionContext* ctxt) {}

    void describe(std::string& descr) const {
        descr = "StopStep";
    }
};

}
