#pragma once

namespace db {

class ExecutionContext;
class Pipeline;

class Executor {
public:
    Executor(ExecutionContext* ctxt, Pipeline* pipeline)
        : _ctxt(ctxt),
        _pipeline(pipeline)
    {
    }

    bool run(bool initRun = false);

    // Call at least once in the life of the server
    // to initialize jump tables
    static void init();

private:
    ExecutionContext* _ctxt {nullptr};
    Pipeline* _pipeline {nullptr};

    void prepare();
    void runImpl(bool initRun);
};

}
