#pragma once

#include "Processor.h"

#include "columns/ColumnVector.h"

namespace db::v2 {

class PipelinePort;
class MaterializeData;

class MaterializeProcessor : public Processor {
public:
    static MaterializeProcessor* create(PipelineV2* pipeline, MaterializeData* matData);

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelinePort* input() const { return _input; }
    PipelinePort* output() const { return _output; }

private:
    PipelinePort* _input {nullptr};
    PipelinePort* _output {nullptr};
    MaterializeData* _matData {nullptr};
    ColumnVector<size_t> _transform;

    MaterializeProcessor(MaterializeData* matData);
    ~MaterializeProcessor();
};

}
