#pragma once

#include <functional>

namespace db {

class ExecutionContext;

class LambdaStep {
public:
    struct Tag {};

    enum class Operation {
        RESET,
        EXECUTE
    };

    using StepFunc = std::function<void(LambdaStep*, Operation)>;

    LambdaStep(StepFunc func)
        : _func(func)
    {
    }

    void prepare(ExecutionContext* ctxt) {}

    bool isFinished() const { return _finished; }

    inline void reset() { _func(this, Operation::RESET); }

    inline void execute() { _func(this, Operation::EXECUTE); }

    void setFinished(bool finished) { _finished = finished; }

private:
    StepFunc _func;
    bool _finished {true};
};

}
