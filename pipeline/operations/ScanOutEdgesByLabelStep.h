#pragma once

#include <memory>
#include <string>

#include "iterators/ScanOutEdgesByLabelIterator.h"
#include "EdgeWriteInfo.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

class ScanOutEdgesByLabelStep {
public:
    struct Tag {};

    ScanOutEdgesByLabelStep(const EdgeWriteInfo& edgeWriteInfo, const LabelSetHandle& labelSet);

    ScanOutEdgesByLabelStep(ScanOutEdgesByLabelStep&& other) = default;

    ~ScanOutEdgesByLabelStep();

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<ScanOutEdgesByLabelChunkWriter>(ctxt->getGraphView(), _labelSet);
        _it->setSrcIDs(_edgeWriteInfo._sourceNodes);
        _it->setEdgeIDs(_edgeWriteInfo._edges);
        _it->setTgtIDs(_edgeWriteInfo._targetNodes);
        _it->setEdgeTypes(_edgeWriteInfo._edgeTypes);
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

    void describe(std::string& descr) const;

private:
    EdgeWriteInfo _edgeWriteInfo;
    LabelSetHandle _labelSet;
    std::unique_ptr<ScanOutEdgesByLabelChunkWriter> _it;
};

}
