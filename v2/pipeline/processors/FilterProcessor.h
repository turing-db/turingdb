#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValuesInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db::v2 {

class FilterProcessor final : public Processor {
public:
    static FilterProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final;

    PipelineBlockInputInterface& toFilterInput() { return _toFilterInput; }
    PipelineValuesInputInterface& maskInput() { return _maskInput; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    PipelineBlockInputInterface _toFilterInput;
    PipelineValuesInputInterface _maskInput;
    PipelineBlockOutputInterface _output;

    FilterProcessor() = default;
    ~FilterProcessor() final = default ;
};

}
