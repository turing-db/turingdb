#pragma once

#include <queue>
#include <stack>

namespace db {

class PipelineV2;
class Processor;
class ExecutionContext;

class PipelineExecutor {
public:
    PipelineExecutor(PipelineV2* pipeline,
                     ExecutionContext* ctxt);
    ~PipelineExecutor();

    void execute();

    // For step by step execution & debugging
    void init();
    void executeCycle();

private:
    PipelineV2* _pipeline {nullptr};
    ExecutionContext* _ctxt {nullptr};
    std::stack<Processor*> _activeStack;
    std::queue<Processor*> _updateQueue;
};

}
