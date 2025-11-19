#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValuesInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db::v2 {

class FilterProcessor : public Processor {
public:
    static FilterProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& toFilterInput() { return _toFilterInput; }
    PipelineValuesInputInterface& maskInput() { return _maskInput; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    PipelineBlockInputInterface _toFilterInput;
    PipelineValuesInputInterface _maskInput;
    PipelineBlockOutputInterface _output;

    FilterProcessor();
    ~FilterProcessor();
};

}
