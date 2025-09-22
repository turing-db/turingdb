#pragma once

#include <vector>

#include "PipelineBuffer.h"

namespace db::v2 {

class PipelineV2;
class ExecutionContext;

class Processor {
public:
    friend PipelineV2;
    using Buffers = std::vector<PipelineBuffer*>;

    const Buffers& inputs() const { return _inputs; }
    const Buffers& outputs() const { return _outputs; }

    bool isFinished() const { return _finished; }

    virtual void prepare(ExecutionContext* ctxt) = 0;
    virtual void reset() = 0;
    virtual void execute() = 0;

    bool isScheduled() const { return _scheduled; }
    void setScheduled(bool scheduled) { _scheduled = scheduled; }

    void connectTo(Processor* target);

    // Checks if the processor can execute
    // 1. All inputs have data (the processor has all the data it needs)
    // 2. All outputs are free (the processor can write its output)
    bool canExecute() const {
        for (const PipelineBuffer* input : _inputs) {
            if (!input->hasData()) {
                return false;
            }
        }

        for (const PipelineBuffer* output : _outputs) {
            if (!output->canWriteData()) {
                return false;
            }
        }

        return true;
    }

protected:
    Buffers _inputs;
    Buffers _outputs;
    bool _scheduled {false};
    bool _finished {false};

    Processor();
    virtual ~Processor();
    void postCreate(PipelineV2* pipeline);
    void addInput(PipelineBuffer* buffer);
    void addOutput(PipelineBuffer* buffer);
};

}
