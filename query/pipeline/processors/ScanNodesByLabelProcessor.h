#pragma once

#include <memory>

#include "Processor.h"

#include "metadata/LabelSet.h"
#include "interfaces/PipelineNodeOutputInterface.h"

namespace db {
class ScanNodesByLabelChunkWriter;
}

namespace db {

class ScanNodesByLabelProcessor : public Processor {
public:
    static ScanNodesByLabelProcessor* create(PipelineV2* pipeline, const LabelSet* labelset);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineNodeOutputInterface& outNodeIDs() { return _outNodeIDs; }

private:
    const LabelSet* _labelset {nullptr};
    PipelineNodeOutputInterface _outNodeIDs;
    std::unique_ptr<ScanNodesByLabelChunkWriter> _it;

    ScanNodesByLabelProcessor(const LabelSet* labelset);
    ~ScanNodesByLabelProcessor();
};

}
