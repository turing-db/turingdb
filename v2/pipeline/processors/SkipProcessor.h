#pragma once

#include "Processor.h"

namespace db::v2 {

class SkipProcessor : public Processor {
public:
    static SkipProcessor* create(PipelineV2* pipeline, size_t skipCount);

    PipelineInputPort* input() const { return _input; }
    PipelineOutputPort* output() const { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineInputPort* _input {nullptr};
    PipelineOutputPort* _output {nullptr};
    const size_t _skipCount {0};
    bool _skipping {true};
    size_t _currentRowCount {0};

    SkipProcessor(size_t skipCount);
    ~SkipProcessor();
};

}
