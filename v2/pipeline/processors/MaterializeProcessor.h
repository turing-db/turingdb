#pragma once

#include "Processor.h"

#include "processors/MaterializeData.h"
#include "columns/ColumnVector.h"

namespace db {
class LocalMemory;
}

namespace db::v2 {

class PipelinePort;

class MaterializeProcessor : public Processor {
public:
    static MaterializeProcessor* create(PipelineV2* pipeline, LocalMemory* mem);

    MaterializeData& getMaterializeData() { return _matData; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineInputPort* input() const { return _input; }
    PipelineOutputPort* output() const { return _output; }

private:
    PipelineInputPort* _input {nullptr};
    PipelineOutputPort* _output {nullptr};
    MaterializeData _matData;
    ColumnVector<size_t> _transform;

    MaterializeProcessor(LocalMemory* mem);
    ~MaterializeProcessor();
};

}
