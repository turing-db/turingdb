#pragma once

#include <memory>

#include "ScanNodesByLabelIterator.h"
#include "ColumnIDs.h"
#include "LabelSet.h"
#include "ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

class ScanNodesByLabelStep {
public:
    struct Tag {};

    ScanNodesByLabelStep(ColumnIDs* nodes, const LabelSet* labelSet);
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
        _it->fill(ChunkConfig::CHUNK_SIZE);
    }

private:
    ColumnIDs* _nodes {nullptr};
    const LabelSet* _labelSet {nullptr};
    std::unique_ptr<ScanNodesByLabelChunkWriter> _it;
};

}
