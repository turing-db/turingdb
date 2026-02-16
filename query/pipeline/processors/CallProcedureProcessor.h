#pragma once

#include <optional>
#include <span>
#include <stdint.h>

#include "Processor.h"
#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "procedures/ProcedureState.h"

namespace db {

class LocalMemory;
class DataframeManager;
class PipelineV2;
class Procedure;

class CallProcedureProcessor : public Processor {
public:
    static CallProcedureProcessor* create(PipelineV2* pipeline,
                                          const Procedure* procedure,
                                          bool hasInput);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    bool hasInput() const { return _input.has_value(); }

    void setInputValues(std::span<const Procedure::Argument> args);
    void allocReturnValues(LocalMemory* mem,
                           DataframeManager* dfMan,
                           std::span<Procedure::YieldItem> yieldItems);

    PipelineBlockInputInterface& input();
    PipelineBlockOutputInterface& output() { return _output; }

    const ProcedureState& getProcedureState() const { return _procedureState; }

private:
    const Procedure* _procedure {nullptr};
    ProcedureState _procedureState;
    std::optional<PipelineBlockInputInterface> _input;
    PipelineBlockOutputInterface _output;

    CallProcedureProcessor();
    ~CallProcedureProcessor();
};

}
