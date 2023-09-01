#pragma once

namespace db::query {

class InterpreterContext;

class ExecutionContext {
public:
    explicit ExecutionContext(InterpreterContext* interpCtxt);
    ~ExecutionContext();

    InterpreterContext* getInterpreterContext() const { return _interpCtxt; }

private:
    InterpreterContext* _interpCtxt {nullptr};
};

}
