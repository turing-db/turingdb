#pragma once

#include <vector>
#include <string>

#include "PipelinePort.h"

namespace db::v2 {

class PipelineV2;
class PipelinePort;
class ExecutionContext;

class Processor {
public:
    friend PipelineV2;
    using InputPorts = std::vector<PipelineInputPort*>;
    using OutputPorts = std::vector<PipelineOutputPort*>;

    virtual std::string describe() const = 0;

    const InputPorts& inputs() const { return _inputs; }
    const OutputPorts& outputs() const { return _outputs; }

    bool isSource() const { return _inputs.empty(); }
    bool isSink() const { return _outputs.empty(); }

    bool isFinished() const { return _finished; }
    bool isPrepared() const { return _prepared; }

    virtual void prepare(ExecutionContext* ctxt) = 0;
    virtual void reset() = 0;
    virtual void execute() = 0;

    bool isScheduled() const { return _scheduled; }
    void setScheduled(bool scheduled) { _scheduled = scheduled; }

    // Checks if the processor can execute
    // 1. All inputs have data (the processor has all the data it needs)
    // 2. All outputs are free (the processor can write its output)
    bool canExecute() const {
        for (const PipelineInputPort* input : _inputs) {
            if (!input->hasData() && input->needsData()) {
                return false;
            }
        }

        for (const PipelineOutputPort* output : _outputs) {
            if (!output->canWriteData()) {
                return false;
            }
        }

        return true;
    }

protected:
    Processor();
    virtual ~Processor();
    void postCreate(PipelineV2* pipeline);
    void addInput(PipelineInputPort* port);
    void addOutput(PipelineOutputPort* port);
    void markAsPrepared() { _prepared = true; }
    void markAsReset() { _finished = false; }
    void finish();

    ExecutionContext* _ctxt {nullptr};

private:
    InputPorts _inputs;
    OutputPorts _outputs;
    bool _scheduled {false};
    bool _finished {false};
    bool _prepared {false};

    bool checkInputsClosed() const;
    void closeOutputs();
};

}
