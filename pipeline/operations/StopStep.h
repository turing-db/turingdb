#pragma once

namespace db {

class ExecutionContext;

class StopStep {
public:
    struct Tag {};

    void prepare(ExecutionContext* ctxt) {}
};

}
