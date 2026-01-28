#pragma once

#include <optional>
#include <span>
#include <stdint.h>

#include "Processor.h"
#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "procedures/Procedure.h"

namespace db {

class LocalMemory;
class DataframeManager;

}

namespace db {

class PipelineV2;
class ProcedureBlueprint;

class DatabaseProcedureProcessor : public Processor {
public:
    static DatabaseProcedureProcessor* create(PipelineV2* pipeline,
                                              const ProcedureBlueprint& blueprint,
                                              bool hasInput);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    bool hasInput() const { return _input.has_value(); }

    void setInputValues(std::span<const ProcedureBlueprint::InputItem> args);
    void allocReturnValues(LocalMemory&,
                           DataframeManager&,
                           std::span<ProcedureBlueprint::YieldItem> yieldItems);

    PipelineBlockInputInterface& input();
    PipelineBlockOutputInterface& output() { return _output; }

    const Procedure& procedure() const { return _procedure; }

private:
    Procedure _procedure;
    std::optional<PipelineBlockInputInterface> _input;
    PipelineBlockOutputInterface _output;

    DatabaseProcedureProcessor();
    ~DatabaseProcedureProcessor();
};

}
