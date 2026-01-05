#pragma once

#include "Processor.h"

#include "columns/ColumnConst.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class CountProcessor : public Processor {
public:
    static CountProcessor* create(PipelineV2* pipeline, ColumnTag colTag);

    std::string describe() const override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineValueOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineBlockInputInterface _input;
    PipelineValueOutputInterface _output;
    size_t _countRunning {0};
    ColumnTag _colTag;
    const Column* _col {nullptr};
    ColumnConst<size_t>* _countColumn {nullptr};

    CountProcessor();
    ~CountProcessor();
};

}
