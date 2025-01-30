#pragma once

#include <memory>

#include "iterators/ScanEdgesIterator.h"
#include "iterators/ChunkConfig.h"
#include "EdgeWriteInfo.h"
#include "ExecutionContext.h"

namespace db {

class ScanEdgesStep {
public:
    struct Tag {};

    ScanEdgesStep(const EdgeWriteInfo& edgeWriteInfo);

    ScanEdgesStep(ScanEdgesStep&& other) = default;
    
    ~ScanEdgesStep();

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<ScanEdgesChunkWriter>(ctxt->getGraphView());
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

private:
    EdgeWriteInfo _edgeWriteInfo;
    std::unique_ptr<ScanEdgesChunkWriter> _it;
};

}
