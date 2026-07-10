#pragma once

#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ID.h"
#include "GraphPath.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

#include "metadata/SupportedType.h"
#include "iterators/GetPropertiesIterator.h"

namespace db {

class GetOutEdgesChunkWriter;

template <typename T>
struct DijkstraNode {
    NodeID id;
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
struct DijkstraNodeComparator {
    bool operator()(const DijkstraNode<T> l, const DijkstraNode<T> r) const {
        return l.distance > r.distance;
    }
};

template <typename T>
struct DijkstraHeapValues {
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
using DijkstraHeap = std::priority_queue<DijkstraNode<T>,
                                         std::vector<DijkstraNode<T>>,
                                         DijkstraNodeComparator<T>>;

template <typename T>
using DijkstraValueMap = std::unordered_map<NodeID, DijkstraHeapValues<T>>;

template <typename T>
struct DijkstraResult {
    NodeID targetNode;
    T distance {0};
    Path path;
};

template <typename T>
struct SubpathCacheEntry {
    NodeID targetNode;
    T distance {0};
    Path pathSuffix;
};

template <typename T>
using SubpathCache = std::unordered_map<NodeID, std::vector<SubpathCacheEntry<T>>>;

template <SupportedType T>
class DijkstraRunner {
public:
    using EdgePropType = T::Primitive;

    DijkstraRunner();
    ~DijkstraRunner();

    void initialize(ColumnNodeIDs* inputNodes,
                    ColumnEdgeIDs* outputEdges,
                    ColumnNodeIDs* outputNodes,
                    ColumnIndices* outputIndices,
                    GetOutEdgesChunkWriter* getOutEdgesWriter,
                    ColumnIndices* propertyIndices,
                    ColumnVector<EdgePropType>* properties,
                    GetPropertiesChunkWriter<EdgeID, T>* getPropertiesWriter);

    void run(const DijkstraHeap<EdgePropType>& initialHeap,
             const DijkstraValueMap<EdgePropType>& initialValues,
             const std::unordered_set<NodeID>& targetNodes,
             bool stopAtFirst,
             const SubpathCache<EdgePropType>* cache = nullptr);

    const std::vector<DijkstraResult<EdgePropType>>& results() const { return _results; }
    const DijkstraValueMap<EdgePropType>& getValueMap() const { return _heapValueMap; }

private:
    DijkstraHeap<EdgePropType> _heap;
    DijkstraValueMap<EdgePropType> _heapValueMap;
    std::unordered_set<NodeID> _settledTargets;
    std::vector<DijkstraResult<EdgePropType>> _results;
    std::unordered_map<NodeID, DijkstraResult<EdgePropType>> _pendingResults;

    ColumnNodeIDs* _inputNodes {nullptr};
    ColumnEdgeIDs* _outputEdges {nullptr};
    ColumnNodeIDs* _outputNodes {nullptr};
    ColumnIndices* _outputIndices {nullptr};
    GetOutEdgesChunkWriter* _getOutEdgesWriter {nullptr};

    ColumnIndices* _propertyIndices {nullptr};
    ColumnVector<EdgePropType>* _properties {nullptr};
    GetPropertiesChunkWriter<EdgeID, T>* _getPropertiesWriter {nullptr};

    void expandNode(const DijkstraNode<EdgePropType>& node);
    void reconstructPath(const DijkstraNode<EdgePropType>& settledNode,
                         Path& outputPath);
    void buildCacheHitPath(const DijkstraNode<EdgePropType>& settledNode,
                           const SubpathCacheEntry<EdgePropType>& cacheEntry,
                           Path& outputPath);
};

}
