#pragma once

namespace db {

class ExecutionContext;

class EndStep {
public:
    struct Tag {};

    void prepare(ExecutionContext* ctxt) {}
};

}
