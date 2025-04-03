#pragma once

#include <memory>
#include <string>

#include "iterators/ScanInEdgesByLabelIterator.h"
#include "EdgeWriteInfo.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

class ScanInEdgesByLabelStep {
public:
    struct Tag {};

    ScanInEdgesByLabelStep(const EdgeWriteInfo& edgeWriteInfo, const LabelSetHandle&);

    ScanInEdgesByLabelStep(ScanInEdgesByLabelStep&& other) = default;

    ~ScanInEdgesByLabelStep();

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<ScanInEdgesByLabelChunkWriter>(ctxt->getGraphView(), _labelSet);
        _it->setSrcIDs(_edgeWriteInfo._targetNodes);
        _it->setEdgeIDs(_edgeWriteInfo._edges);
        _it->setTgtIDs(_edgeWriteInfo._sourceNodes);
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
    std::unique_ptr<ScanInEdgesByLabelChunkWriter> _it;
};

}
