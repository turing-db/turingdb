#pragma once

#include "Processor.h"

namespace db::v2 {

class LimitProcessor : public Processor {
public:
    static LimitProcessor* create(PipelineV2* pipeline, size_t limit);

    PipelineInputPort* input() const { return _input; }
    PipelineOutputPort* output() const { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineInputPort* _input {nullptr};
    PipelineOutputPort* _output {nullptr};
    size_t _limit {0};
    size_t _currentRowCount {0};
    bool _reachedLimit {false};

    LimitProcessor(size_t limit);
    ~LimitProcessor() override;
};

}
