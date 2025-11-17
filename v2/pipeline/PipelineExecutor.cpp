#include "PipelineExecutor.h"

#include "Processor.h"
#include "PipelineV2.h"
#include "PipelineBuffer.h"

using namespace db::v2;

PipelineExecutor::PipelineExecutor(PipelineV2* pipeline,
                                   ExecutionContext* ctxt)
    : _pipeline(pipeline),
      _ctxt(ctxt)
{
}

PipelineExecutor::~PipelineExecutor() {
}

void PipelineExecutor::init() {
    // Add all sources to the active stack
    auto& activeStack = _activeStack;
    for (Processor* source : _pipeline->sources()) {
        activeStack.push(source);
    }
}

void PipelineExecutor::execute() {
    init();
    while (!_activeStack.empty()) {
        executeCycle();
    }
}

void PipelineExecutor::executeCycle() {
    auto& activeStack = _activeStack;
    auto& updateQueue = _updateQueue;

    // Search for an active processor in the active stack
    {
        while (!_activeStack.empty()) {
            Processor* proc = _activeStack.top();
            _activeStack.pop();

            if (proc->canExecute()) {
                _updateQueue.push(proc);
                break;
            }
        }
    }

    // Execute processors
    while (!updateQueue.empty()) {
        Processor* currentProc = updateQueue.front();
        updateQueue.pop();

        if (!currentProc->isPrepared()) {
            currentProc->prepare(_ctxt);
        }

        if (currentProc->isFinished()) {
            currentProc->reset();
        }

        currentProc->execute();

        // If the processor is not finished in one step, add to the active queue
        if (!currentProc->isFinished()) {
            activeStack.push(currentProc);
        }

        currentProc->setScheduled(false);

        // Add all successors to the update queue if they are ready to execute
        for (PipelinePort* output : currentProc->outputs()) {
            PipelinePort* connectedPort = output->getConnectedPort();
            if (connectedPort) {
                Processor* nextProc = connectedPort->getProcessor();
                if (!nextProc->isScheduled() && nextProc->canExecute()) {
                    updateQueue.push(nextProc);
                    nextProc->setScheduled(true);
                }
            }
        }
    }
}
