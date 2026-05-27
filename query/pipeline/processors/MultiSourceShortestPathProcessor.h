#pragma once

#include <queue>
#include <unordered_set>
#include <unordered_map>

#include "ID.h"
#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"
#include "iterators/GetPropertiesIterator.h"

namespace db {

class GraphView;
class GetOutEdgesChunkWriter;
class LocalMemory;
class PipelineV2;

template <typename T>
struct MultiSourceDijkstraNode {
    NodeID id;
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
struct MultiSourceDijkstraNodeComparator {
    bool operator()(const MultiSourceDijkstraNode<T> l, const MultiSourceDijkstraNode<T> r) const {
        return l.distance > r.distance;
    }
};

template <typename T>
struct MultiSourceHeapMapValues {
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <SupportedType T>
class MultiSourceShortestPathProcessor final : public Processor {
public:
    using EdgePropType = T::Primitive;
    using DijkstraHeap = std::priority_queue<MultiSourceDijkstraNode<EdgePropType>,
                                             std::vector<MultiSourceDijkstraNode<EdgePropType>>,
                                             MultiSourceDijkstraNodeComparator<EdgePropType>>;

    using DijkstraValueMap = std::unordered_map<NodeID, MultiSourceHeapMapValues<EdgePropType>>;

    static MultiSourceShortestPathProcessor<T>* create(PipelineV2* pipeline,
                                                       LocalMemory* mem,
                                                       ColumnTag sourceTag,
                                                       ColumnTag targetTag,
                                                       const PropertyType& edgeType);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final {
        return "MultiSourceShortestPathProcessor";
    }

    PipelineBlockInputInterface& leftHandSide() { return _source; }
    PipelineBlockInputInterface& rightHandSide() { return _target; }
    PipelineBlockOutputInterface& output() { return _out; }

    void addSourceOutputTag(ColumnTag tag) { _sourceOutputTag = tag; }
    void addTargetOutputTag(ColumnTag tag) { _targetOutputTag = tag; }
    void addDistVarTag(ColumnTag distTag) { _distTag = distTag; }
    void addPathVarTag(ColumnTag pathTag) { _pathTag = pathTag; }

private:
    LocalMemory* _mem {nullptr};

    MultiSourceShortestPathProcessor(LocalMemory* mem,
                                     ColumnTag sourceTag,
                                     ColumnTag targetTag,
                                     const PropertyType& edgeType);
    ~MultiSourceShortestPathProcessor() final = default;

    PipelineBlockInputInterface _source;
    PipelineBlockInputInterface _target;
    PipelineBlockOutputInterface _out;

    ColumnTag _sourceColumn;
    ColumnTag _targetColumn;
    ColumnTag _sourceOutputTag;
    ColumnTag _targetOutputTag;
    ColumnTag _distTag;
    ColumnTag _pathTag;
    PropertyType _edgeType;

    ColumnNodeIDs* _input {nullptr};
    ColumnEdgeIDs* _outputEdges {nullptr};
    ColumnNodeIDs* _outputNodes {nullptr};
    ColumnIndices* _outputIndices {nullptr};
    std::unique_ptr<GetOutEdgesChunkWriter> _getOutEdgesWriter;

    ColumnIndices* _propertyIndices {nullptr};
    ColumnVector<EdgePropType>* _properties {nullptr};
    std::unique_ptr<GetPropertiesChunkWriter<EdgeID, T>> _getPropertiesWriter;

    std::vector<NodeID> _sourceNodes;
    std::unordered_set<NodeID> _targetNodes;

    void runDijkstra(NodeID sourceNode,
                     ColumnVector<NodeID>* sourceOutputCol,
                     ColumnVector<NodeID>* targetOutputCol,
                     ColumnVector<EdgePropType>* distCol,
                     ColumnVector<Path>* pathCol);
};

}
