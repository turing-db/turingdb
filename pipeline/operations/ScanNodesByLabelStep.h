#pragma once

#include <memory>
#include <string>

#include "Profiler.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "columns/ColumnIDs.h"
#include "metadata/LabelSet.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

class ScanNodesByLabelStep {
public:
    struct Tag {};

    ScanNodesByLabelStep(ColumnNodeIDs* nodes, const LabelSetHandle& labelSet);
    ScanNodesByLabelStep(ScanNodesByLabelStep&& other) = default;
    ~ScanNodesByLabelStep();

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<ScanNodesByLabelChunkWriter>(ctxt->getGraphView(), _labelSet);
        _it->setNodeIDs(_nodes);
    }

    inline void reset() {
        _it->reset();
    }

    inline bool isFinished() const {
        return !_it->isValid();
    }

    inline void execute() {
        Profile profile {"ScanNodesByLabelStep::execute"};

        _it->fill(ChunkConfig::CHUNK_SIZE);
    }

    void describe(std::string& descr) const;

private:
    ColumnNodeIDs* _nodes {nullptr};
    LabelSetHandle _labelSet;
    std::unique_ptr<ScanNodesByLabelChunkWriter> _it;
};

}
