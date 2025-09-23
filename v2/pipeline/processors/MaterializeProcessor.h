#pragma once

#include "Processor.h"

#include "columns/ColumnVector.h"

namespace db::v2 {

class PipelineBuffer;
class MaterializeData;

class MaterializeProcessor : public Processor {
public:
    static MaterializeProcessor* create(PipelineV2* pipeline, MaterializeData* matData);

    void prepare(ExecutionContext* ctxt) override;

    void reset() override;

    void execute() override;

    void createStep();

private:
    PipelineBuffer* _input {nullptr};
    PipelineBuffer* _output {nullptr};
    MaterializeData* _matData {nullptr};
    ColumnVector<size_t> _transform;

    MaterializeProcessor(MaterializeData* matData);
    ~MaterializeProcessor();
};

}
