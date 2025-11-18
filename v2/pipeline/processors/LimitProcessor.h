#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

namespace db::v2 {

class LimitProcessor : public Processor {
public:
    static LimitProcessor* create(PipelineV2* pipeline, size_t limit);

    std::string describe() const override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;
    size_t _limit {0};
    size_t _currentRowCount {0};
    bool _reachedLimit {false};

    LimitProcessor(size_t limit);
    ~LimitProcessor() override;
};

}
