#pragma once

#include <memory>
#include <shared_mutex>
#include <string>

#include "DataPart.h"
#include "EntityID.h"

class JobSystem;

namespace db {

class GraphInfoLoader;
class GraphView;
class ConcurrentWriter;
class DataPartBuilder;
class PartIterator;
class GraphMetadata;
class DataPartManager;
class GraphReader;
class GraphLoader;

class Graph {
public:
    friend GraphView;

    struct EntityIDs {
        EntityID _node {0};
        EntityID _edge {0};
    };

    Graph();
    explicit Graph(const std::string& name);
    ~Graph();

    Graph(const Graph&) = delete;
    Graph(Graph&&) = delete;
    Graph& operator=(const Graph&) = delete;
    Graph& operator=(Graph&&) = delete;

    const std::string& getName() const { return _graphName; }

    [[nodiscard]] GraphView view();
    [[nodiscard]] GraphView view() const;
    [[nodiscard]] GraphReader read();
    [[nodiscard]] GraphReader read() const;
    [[nodiscard]] std::unique_ptr<DataPartBuilder> newPartWriter();
    [[nodiscard]] std::unique_ptr<ConcurrentWriter> newConcurrentPartWriter();

    [[nodiscard]] const GraphMetadata* getMetadata() const { return _metadata.get(); }
    [[nodiscard]] GraphMetadata* getMetadata() { return _metadata.get(); }

    [[nodiscard]] EntityIDs getNextFreeIDs() const;

private:
    friend GraphInfoLoader;
    friend PartIterator;
    friend DataPartBuilder;
    friend ConcurrentWriter;
    friend GraphLoader;

    std::string _graphName;
    std::unique_ptr<GraphMetadata> _metadata;

    mutable std::shared_mutex _entityIDsMutex;
    EntityIDs _nextFreeIDs;

    mutable std::shared_mutex _mainLock;

    std::unique_ptr<DataPartManager> _parts;

    EntityIDs allocIDs();
    EntityIDs allocIDRange(size_t nodeCount, size_t edgeCount);
};

}
