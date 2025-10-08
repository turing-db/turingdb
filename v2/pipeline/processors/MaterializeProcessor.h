#pragma once

#include "Processor.h"

#include "columns/ColumnVector.h"

namespace db::v2 {

class PipelinePort;
class MaterializeData;

class MaterializeProcessor : public Processor {
public:
    static MaterializeProcessor* create(PipelineV2* pipeline, MaterializeData* matData);

    std::string_view getName() const override;

    MaterializeData* getMaterializeData() { return _matData; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineInputPort* input() const { return _input; }
    PipelineOutputPort* output() const { return _output; }

private:
    PipelineInputPort* _input {nullptr};
    PipelineOutputPort* _output {nullptr};
    MaterializeData* _matData {nullptr};
    ColumnVector<size_t> _transform;

    MaterializeProcessor(MaterializeData* matData);
    ~MaterializeProcessor();
};

}
