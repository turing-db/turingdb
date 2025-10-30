#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

#include "processors/MaterializeData.h"
#include "columns/ColumnVector.h"

namespace db {
class LocalMemory;
}

namespace db::v2 {

class MaterializeProcessor : public Processor {
public:
    static MaterializeProcessor* create(PipelineV2* pipeline, LocalMemory* mem);

    MaterializeData& getMaterializeData() { return _matData; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineInputInterface& input() { return _input; }
    PipelineOutputInterface& output() { return _output; }

private:
    PipelineInputInterface _input;
    PipelineOutputInterface _output;
    MaterializeData _matData;
    ColumnVector<size_t> _transform;

    MaterializeProcessor(LocalMemory* mem);
    ~MaterializeProcessor();
};

}
