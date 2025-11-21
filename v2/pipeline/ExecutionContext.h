#pragma once

#include "iterators/ChunkConfig.h"

namespace db {
class GraphView;
class Transaction;
}

namespace db::v2 {

class ExecutionContext {
public:
    ExecutionContext(const GraphView& graphView, Transaction* tx = nullptr)
        : _graphView(graphView),
        _tx {tx}
    {
    }

    const GraphView& getGraphView() const { return _graphView; }
    size_t getChunkSize() const { return _chunkSize; }
    Transaction* getTransaction() { return _tx; }

    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

private:
    const GraphView& _graphView;
    size_t _chunkSize = ChunkConfig::CHUNK_SIZE;
    Transaction* _tx {nullptr};
};
}
