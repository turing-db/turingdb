#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

#include "processors/MaterializeData.h"
#include "columns/ColumnVector.h"

namespace db {
class LocalMemory;
class DataframeManager;
}

namespace db::v2 {

class MaterializeProcessor : public Processor {
public:
    static MaterializeProcessor* create(PipelineV2* pipeline, LocalMemory* mem);

    MaterializeData& getMaterializeData() { return _matData; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;
    MaterializeData _matData;
    ColumnVector<size_t> _transform;

    MaterializeProcessor(LocalMemory* mem, DataframeManager* dfMan);
    ~MaterializeProcessor();
};

}
