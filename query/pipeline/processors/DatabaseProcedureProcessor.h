#pragma once

#include <span>
#include <stdint.h>

#include "Processor.h"
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
                                              const ProcedureBlueprint& blueprint);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    void setInputValues(std::span<const ProcedureBlueprint::InputItem> args);
    void allocReturnValues(LocalMemory&,
                           DataframeManager&,
                           std::span<ProcedureBlueprint::YieldItem> yieldItems);

    PipelineBlockOutputInterface& output() { return _output; }

    const Procedure& procedure() const { return _procedure; }

private:
    Procedure _procedure;
    PipelineBlockOutputInterface _output;

    DatabaseProcedureProcessor();
    ~DatabaseProcedureProcessor();
};

}
