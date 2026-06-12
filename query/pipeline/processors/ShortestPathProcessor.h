#pragma once

#include <unordered_set>

#include "ID.h"
#include "Processor.h"
#include "ShortestPathUtils.h"

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

template <SupportedType T>
class ShortestPathProcessor final : public Processor {
public:
    using EdgePropType = T::Primitive;

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

    void addDistVarTag(ColumnTag distTag) { _distTag = distTag; }
    void addPathVarTag(ColumnTag pathTag) { _pathTag = pathTag; }

private:
    LocalMemory* _mem {nullptr};

    ShortestPathProcessor(LocalMemory* mem,
                          ColumnTag sourceTag,
                          ColumnTag targetTag,
                          const PropertyType& edgeType);
    ~ShortestPathProcessor();

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
    DijkstraHeap<EdgePropType> _heap;
    DijkstraValueMap<EdgePropType> _heapValueMap;

    DijkstraRunner<T> _runner;
};

}
