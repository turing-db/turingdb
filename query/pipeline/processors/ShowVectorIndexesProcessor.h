#pragma once

#include "Processor.h"

#include <string>
#include <vector>

#include "interfaces/PipelineValuesOutputInterface.h"

namespace db {

class ShowVectorIndexesProcessor : public Processor {
public:
    static ShowVectorIndexesProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineValuesOutputInterface& outNames() { return _outNames; }
    PipelineValuesOutputInterface& outDimensions() { return _outDimensions; }

protected:
    PipelineValuesOutputInterface _outNames;
    PipelineValuesOutputInterface _outDimensions;
    std::vector<std::string> _indexNames;

    ShowVectorIndexesProcessor();
    ~ShowVectorIndexesProcessor();
};

}
