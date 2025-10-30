#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

namespace db::v2 {

class SkipProcessor : public Processor {
public:
    static SkipProcessor* create(PipelineV2* pipeline, size_t skipCount);

    PipelineInputInterface& input() { return _input; }
    PipelineOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineInputInterface _input;
    PipelineOutputInterface _output;
    const size_t _skipCount {0};
    bool _skipping {true};
    size_t _currentRowCount {0};

    SkipProcessor(size_t skipCount);
    ~SkipProcessor();
};

}
