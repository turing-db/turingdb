#pragma once

#include <queue>

namespace db::v2 {

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
    void executeStep();

private:
    PipelineV2* _pipeline {nullptr};
    ExecutionContext* _ctxt {nullptr};
    std::queue<Processor*> _activeQueue;
    std::queue<Processor*> _updateQueue;
};

}
