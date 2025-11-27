#pragma once

#include <stdint.h>

#include "Processor.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db::v2 {

class PipelineV2;

class Procedure {
public:
    enum class Step : uint8_t {
        PREPARE = 0,
        RESET,
        EXECUTE,
    };

    void finish() { _finished = true; }
    void setStep(Step step) { _step = step; }
    void setContext(const ExecutionContext* ctxt) { _ctxt = ctxt; }
    void setOutput(PipelineBlockOutputInterface& output) { _output = &output; }
    void writeOutputData() { _output->getPort()->writeData(); }

    bool isFinished() const { return _finished; }
    Step step() const { return _step; }
    const ExecutionContext* ctxt() const { return _ctxt; }

private:
    const ExecutionContext* _ctxt {nullptr};
    PipelineBlockOutputInterface* _output {nullptr};
    bool _finished {false};
    Step _step {};
};

class ProcedureProcessor : public Processor {
public:
    enum class OperationStep : uint8_t {
        PREPARE = 0,
        RESET,
        EXECUTE,
    };

    struct Operation {
        OperationStep _step {};
    };

    using Callback = std::function<void(Procedure&)>;

    static ProcedureProcessor* create(PipelineV2* pipeline, const Callback& callback);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockOutputInterface& output() { return _output; }

private:
    Callback _callback;
    Procedure _procedure;
    PipelineBlockOutputInterface _output;

    explicit ProcedureProcessor(const Callback& callback);
    ~ProcedureProcessor();
};

}
