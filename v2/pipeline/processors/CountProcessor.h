#pragma once

#include "Processor.h"

#include "columns/ColumnConst.h"

namespace db::v2 {

class CountProcessor : public Processor {
public:
    static CountProcessor* create(PipelineV2* pipeline);

    PipelineInputPort* input() const { return _input; }
    PipelineOutputPort* output() const { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineInputPort* _input {nullptr};
    PipelineOutputPort* _output {nullptr};
    ColumnConst<size_t>* _countColumn {nullptr};
    size_t _countRunning {0};

    CountProcessor();
    ~CountProcessor();
};

}
