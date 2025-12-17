#pragma once

#include "Processor.h"

#include "columns/ColumnOptVector.h"
#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "metadata/PropertyType.h"

namespace db::v2 {

class ExprProgram;

class FilterProcessor final : public Processor {
public:
    static FilterProcessor* create(PipelineV2* pipeline, ExprProgram* exprProg);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    using ColumnOptMask = ColumnOptVector<types::Bool::Primitive>;

    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    ExprProgram* _exprProg {nullptr};

    FilterProcessor(ExprProgram* exprProg);
    ~FilterProcessor() final = default ;
};

}
