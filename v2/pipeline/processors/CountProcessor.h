#pragma once

#include "Processor.h"

#include "columns/ColumnConst.h"

#include "PipelineInterface.h"

namespace db::v2 {

class CountProcessor : public Processor {
public:
    static CountProcessor* create(PipelineV2* pipeline);

    PipelineInputInterface& input() { return _input; }
    PipelineOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineInputInterface _input;
    PipelineOutputInterface _output;
    size_t _countRunning {0};
    ColumnConst<size_t>* _countColumn {nullptr};

    CountProcessor();
    ~CountProcessor();
};

}
