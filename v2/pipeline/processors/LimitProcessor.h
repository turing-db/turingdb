#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

namespace db::v2 {

class LimitProcessor : public Processor {
public:
    static LimitProcessor* create(PipelineV2* pipeline, size_t limit);

    PipelineInputInterface& input() { return _input; }
    PipelineOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineInputInterface _input;
    PipelineOutputInterface _output;
    size_t _limit {0};
    size_t _currentRowCount {0};
    bool _reachedLimit {false};

    LimitProcessor(size_t limit);
    ~LimitProcessor() override;
};

}
