#pragma once

#include <memory>
#include <string>

#include "Profiler.h"
#include "iterators/ScanNodesIterator.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

class ScanNodesStep {
public:
    struct Tag {};

    ScanNodesStep(ColumnNodeIDs* nodes);
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
        Profile profile {"ScanNodesStep::execute"};

        _it->fill(ChunkConfig::CHUNK_SIZE);
    }

    void describe(std::string& descr) const;

private:
    ColumnNodeIDs* _nodes {nullptr};
    std::unique_ptr<ScanNodesChunkWriter> _it {nullptr};
};

}
