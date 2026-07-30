#pragma once

#include <optional>
#include <span>

#include "Processor.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class LocalMemory;
class DataframeManager;
class PipelineV2;
class Procedure;

class CallProcedureProcessor final : public Processor {
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

    NamedColumn* allocIndices(LocalMemory* mem, DataframeManager* dfMan);

    PipelineBlockInputInterface& input();
    PipelineBlockOutputInterface& output() { return _output; }

    const ProcedureState& getProcedureState() const { return _procedureState; }

private:
    const Procedure* _procedure {nullptr};
    ProcedureContext _procedureContext;
    ProcedureState _procedureState;
    std::optional<PipelineBlockInputInterface> _input;
    PipelineBlockOutputInterface _output;

    CallProcedureProcessor();
    ~CallProcedureProcessor() final;
};

}
