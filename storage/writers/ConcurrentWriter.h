#pragma once

#include <memory>
#include <vector>

#include "DataPartBuilder.h"

class JobSystem;

namespace db {

class ConcurrentWriter {
public:
    ConcurrentWriter(EntityID firstNodeID, EntityID firstEdgeID, Graph* graph);

    ConcurrentWriter(const ConcurrentWriter&) = delete;
    ConcurrentWriter(ConcurrentWriter&&) = delete;
    ConcurrentWriter& operator=(const ConcurrentWriter&) = delete;
    ConcurrentWriter& operator=(ConcurrentWriter&&) = delete;
    ~ConcurrentWriter();

    DataPartBuilder& newBuilder(size_t nodeCount, size_t edgeCount);
    std::span<std::unique_ptr<DataPartBuilder>> builders() { return _builders; }
    void clear() { _builders.clear(); }

    void commitAll(JobSystem& jobSystem);

private:
    std::mutex _mutex;
    EntityID _firstNodeID {0};
    EntityID _firstEdgeID {0};
    EntityID _nextNodeID {0};
    EntityID _nextEdgeID {0};
    Graph* _graph {nullptr};

    std::vector<std::unique_ptr<DataPartBuilder>> _builders;
};

}
