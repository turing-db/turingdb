#pragma once

#include <queue>
#include <unordered_set>

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

template <typename T>
struct DjistrakaNode {
    NodeID id;
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
struct HeapMapValues {
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
struct DjistrakaNodeCompartor {
    bool operator()(const DjistrakaNode<T> l, const DjistrakaNode<T> r) const {
        return l.distance > r.distance;
    }
};

class PipelineV2;

template <SupportedType T>
class ShortestPathProcessor final : public Processor {
public:
    using EdgePropType = T::Primitive;
    using DjistrakaHeap = std::priority_queue<DjistrakaNode<EdgePropType>,
                                              std::vector<DjistrakaNode<EdgePropType>>,
                                              DjistrakaNodeCompartor<EdgePropType>>;

    using DjistrakaValueMap = std::unordered_map<NodeID, HeapMapValues<EdgePropType>>;

    static ShortestPathProcessor<T>* create(PipelineV2* pipeline,
                                            LocalMemory* mem,
                                            ColumnTag sourceTag,
                                            ColumnTag targetTag,
                                            const PropertyType& edgeType);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final {
        return "ShortestPathProcessor";
    }

    PipelineBlockInputInterface& leftHandSide() { return _source; }
    PipelineBlockInputInterface& rightHandSide() { return _target; }
    PipelineBlockOutputInterface& output() { return _out; }

    void addDistVarTag(ColumnTag distTag) { _distTag = distTag; };
    void addPathVarTag(ColumnTag pathTag) { _pathTag = pathTag; };

private:
    LocalMemory* _mem {nullptr};

    ShortestPathProcessor(LocalMemory* mem,
                          ColumnTag sourceTag,
                          ColumnTag targetTag,
                          const PropertyType& edgeType);
    ;
    ~ShortestPathProcessor() final = default;

    PipelineBlockInputInterface _source;
    PipelineBlockInputInterface _target;
    PipelineBlockOutputInterface _out;

    ColumnTag _sourceColumn;
    ColumnTag _targetColumn;
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

    std::unordered_set<NodeID> _targetNodes;
    DjistrakaHeap _heap;
    DjistrakaValueMap _heapValueMap;
};

}
