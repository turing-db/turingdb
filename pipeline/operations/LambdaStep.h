#pragma once

#include <functional>
#include <string>

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

    void describe(std::string& descr) const;

private:
    StepFunc _func;
    bool _finished {true};
};

}
