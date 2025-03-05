#pragma once

#include <memory>
#include <string>

#include "iterators/ScanNodesIterator.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

class ColumnID;

class ScanNodesStep {
public:
    struct Tag {};

    ScanNodesStep(ColumnIDs* nodes);
    ScanNodesStep(ScanNodesStep&& other) = default;
    ~ScanNodesStep();

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<ScanNodesChunkWriter>(ctxt->getGraphView());
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

    void describe(std::string& descr) const;

private:
    ColumnIDs* _nodes {nullptr};
    std::unique_ptr<ScanNodesChunkWriter> _it {nullptr};
};

}
