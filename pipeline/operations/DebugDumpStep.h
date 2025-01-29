#pragma once

#include <ostream>

namespace db {

class Block;
class ExecutionContext;

class DebugDumpStep {
public:
    struct Tag {};

    DebugDumpStep(const Block* input,
                  std::ostream& out)
        : _input(input),
        _out(out)
    {
    }

    void prepare(ExecutionContext* ctxt) {}

    inline void reset() {}

    inline bool isFinished() const { return true; }

    void execute();

private:
    const Block* _input {nullptr};
    std::ostream& _out;
};

}
