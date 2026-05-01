#pragma once

#include <string_view>

#include "iterators/ChunkConfig.h"

#include "SystemManager.h"

namespace db {

class GraphView;
class Transaction;
class Graph;

class ExecutionContext {
public:
    ExecutionContext(SystemManager* sysMan,
                     const GraphView& graphView)
        : _sysMan(sysMan),
        _graphView(graphView)
    {
    }

    const GraphView& getGraphView() const { return _graphView; }
    size_t getChunkSize() const { return _chunkSize; }
    Transaction* getTransaction() { return _tx; }
    std::string_view getGraphName() const { return _graphName; }
    SystemManager* getSystemManager() const { return _sysMan; }
    JobSystem* getJobSystem() const { return _sysMan->getJobSystem(); }
    const ProcedureManager* getProcedures() const { return _sysMan->getProcedures(); }
    ExtensionManager* getExtensions() const { return _sysMan->getExtensions(); }
    vec::VectorDatabase* getVectorDatabase() const { return _sysMan->getVectorDatabase(); }

    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }
    void setTransaction(Transaction* tx) { _tx = tx; }
    void setGraphName(std::string_view graphName) { _graphName = graphName; }

private:
    SystemManager* _sysMan {nullptr};
    const GraphView& _graphView;
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    Transaction* _tx {nullptr};
    std::string_view _graphName;
};

}
