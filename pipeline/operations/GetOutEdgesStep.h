#pragma once

#include <memory>
#include <string>

#include "Profiler.h"
#include "iterators/GetOutEdgesIterator.h"
#include "EdgeWriteInfo.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

class GetOutEdgesChunkWriter;

class GetOutEdgesStep {
public:
    struct Tag {};
    
    GetOutEdgesStep(const ColumnNodeIDs* inputNodeIDs,
                    const EdgeWriteInfo& edgeWriteInfo);
    GetOutEdgesStep(GetOutEdgesStep&& other) = default;
    ~GetOutEdgesStep();

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<GetOutEdgesChunkWriter>(ctxt->getGraphView(), _inputNodeIDs);
        _it->setIndices(_edgeWriteInfo._indices);
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
        Profile profile {"GetOutEdgesStep::execute"};

        _it->fill(ChunkConfig::CHUNK_SIZE);
    }

    void describe(std::string& descr) const;

private:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    EdgeWriteInfo _edgeWriteInfo;
    std::unique_ptr<GetOutEdgesChunkWriter> _it;
};

}
