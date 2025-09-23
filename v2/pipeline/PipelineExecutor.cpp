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
    // Prepare all processors
    for (Processor* processor : _pipeline->processors()) {
        processor->prepare(_ctxt);
    }

    // Add all sources to the active queue
    auto& activeQueue = _activeQueue;
	for (Processor* source : _pipeline->sources()) {
		activeQueue.push(source);
	}
}

void PipelineExecutor::execute() {
    init();
    while (!_activeQueue.empty()) {
        executeStep();
    }
}

void PipelineExecutor::executeStep() {
    auto& activeQueue = _activeQueue;
    auto& updateQueue = _updateQueue;

    // Search for active processors that are ready to execute
    const size_t activeCount = activeQueue.size();
    for (size_t i = 0; i < activeCount; i++) {
        Processor* processor = activeQueue.front();
        activeQueue.pop();

        if (processor->canExecute()) {
            updateQueue.push(processor);
        } else {
            activeQueue.push(processor);
        }
    }

    // Execute processors
    while (!updateQueue.empty()) {
        Processor* currentProc = updateQueue.front();
        updateQueue.pop();

        // We need to reset processors that were finished
        // and that are reactivated by new data
        if (currentProc->isFinished()) {
            currentProc->reset();
        }

        currentProc->execute();

        // If the processor is not finished in one step, add to the active queue
        if (!currentProc->isFinished()) {
            activeQueue.push(currentProc);
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
