#pragma once

#include <string_view>

#include "iterators/ChunkConfig.h"

namespace db {
class SystemManager;
class GraphView;
class Transaction;
class SystemManager;
class JobSystem;
class Graph;
}

namespace db::v2 {

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
    JobSystem* getJobSystem() const { return _jobSystem; }
    SystemManager* getSystemManager() const { return _sysMan; }

    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }
    void setTransaction(Transaction* tx) { _tx = tx; }
    void setGraphName(std::string_view graphName) { _graphName = graphName; }
    void setJobSystem(JobSystem* jobSystem) { _jobSystem = jobSystem; }

private:
    SystemManager* _sysMan {nullptr};
    const GraphView& _graphView;
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    Transaction* _tx {nullptr};
    std::string_view _graphName;
    JobSystem* _jobSystem {nullptr};
};

}
