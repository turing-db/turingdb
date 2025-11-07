#pragma once

#include <memory>

#include "Processor.h"

#include "PipelineInterface.h"

class Dataframe;

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

    size_t _lhsTotalRowCount;
    size_t _rhsTotalRowCount;

    size_t _lhsRowPtr;
    size_t _rhsRowPtr;
};

}

