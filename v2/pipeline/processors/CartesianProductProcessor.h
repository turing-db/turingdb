#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

namespace db::v2 {

class PipelineV2;

class CartesianProductProcessor : public Processor {
public:
    static CartesianProductProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    CartesianProductProcessor();
    ~CartesianProductProcessor() override;

    PipelineBlockInputInterface _lhs;
    PipelineBlockInputInterface _rhs;
    PipelineBlockOutputInterface _out;
};
  
}
