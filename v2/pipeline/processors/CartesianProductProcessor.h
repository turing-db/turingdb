#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

namespace db {

class Dataframe;

}

namespace db::v2 {

class PipelineV2;

class CartesianProductProcessor final : public Processor {
public:
    static CartesianProductProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockInputInterface& leftHandSide() { return _lhs; }
    PipelineBlockInputInterface& rightHandSide() { return _rhs; }
    PipelineBlockOutputInterface& output() { return _out; }

private:
    CartesianProductProcessor();
    ~CartesianProductProcessor() final;

    PipelineBlockInputInterface _lhs;
    PipelineBlockInputInterface _rhs;
    PipelineBlockOutputInterface _out;

    size_t _lhsTotalRowCount {0};
    size_t _rhsTotalRowCount {0};

    size_t _lhsRowPtr {0};
    size_t _rhsRowPtr {0};
};

}

