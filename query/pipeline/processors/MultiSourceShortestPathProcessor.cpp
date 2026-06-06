#include "MultiSourceShortestPathProcessor.h"

#include <algorithm>

#include "LocalMemory.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetPropertiesIterator.h"
#include "dataframe/Dataframe.h"

using namespace db;

template <SupportedType T>
MultiSourceShortestPathProcessor<T>::MultiSourceShortestPathProcessor(LocalMemory* mem,
                                                                      ColumnTag sourceTag,
                                                                      ColumnTag targetTag,
                                                                      const PropertyType& edgeType)
    : _mem(mem),
    _sourceColumn(sourceTag),
    _targetColumn(targetTag),
    _edgeType(edgeType)
{
}

template <SupportedType T>
MultiSourceShortestPathProcessor<T>::~MultiSourceShortestPathProcessor() = default;

template <SupportedType T>
MultiSourceShortestPathProcessor<T>* MultiSourceShortestPathProcessor<T>::create(PipelineV2* pipeline,
                                                                                 LocalMemory* mem,
                                                                                 ColumnTag sourceTag,
                                                                                 ColumnTag targetTag,
                                                                                 const PropertyType& edgeType) {
    auto* processor = new MultiSourceShortestPathProcessor(mem,
                                                           sourceTag,
                                                           targetTag,
                                                           edgeType);

    {
        PipelineInputPort* sourceInputPort = PipelineInputPort::create(pipeline, processor);
        processor->_source.setPort(sourceInputPort);
        processor->addInput(sourceInputPort);
        sourceInputPort->setNeedsData(false);
    }

    {
        PipelineInputPort* targetInputPort = PipelineInputPort::create(pipeline, processor);
        processor->_target.setPort(targetInputPort);
        processor->addInput(targetInputPort);
        targetInputPort->setNeedsData(false);
    }

    {
        PipelineOutputPort* output = PipelineOutputPort::create(pipeline, processor);
        processor->_out.setPort(output);
        processor->addOutput(output);
    }

    processor->postCreate(pipeline);
    return processor;
}

template <SupportedType T>
void MultiSourceShortestPathProcessor<T>::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    const GraphView& view = _ctxt->getGraphView();

    _input = _mem->alloc<ColumnNodeIDs>();
    _outputEdges = _mem->alloc<ColumnEdgeIDs>();
    _outputNodes = _mem->alloc<ColumnNodeIDs>();
    _outputIndices = _mem->alloc<ColumnIndices>();

    _getOutEdgesWriter = std::make_unique<GetOutEdgesChunkWriter>(view, _input);
    _getOutEdgesWriter->setIndices(_outputIndices);
    _getOutEdgesWriter->setEdgeIDs(_outputEdges);
    _getOutEdgesWriter->setTgtIDs(_outputNodes);

    _propertyIndices = _mem->alloc<ColumnIndices>();
    _properties = _mem->alloc<ColumnVector<EdgePropType>>();
    _getPropertiesWriter = std::make_unique<GetPropertiesChunkWriter<EdgeID, T>>(view,
                                                                                 _edgeType._id,
                                                                                 _outputEdges);
    _getPropertiesWriter->setOutput(_properties);
    _getPropertiesWriter->setIndices(_propertyIndices);

    markAsPrepared();
}

template <SupportedType T>
void MultiSourceShortestPathProcessor<T>::reset() {
    markAsReset();
}

template <SupportedType T>
void MultiSourceShortestPathProcessor<T>::execute() {
    if (_target.getPort()->hasData()) {
        const Dataframe* targetDf = _target.getDataframe();
        auto* col = targetDf->getColumn<ColumnNodeIDs>(_targetColumn);
        if (!col) {
            throw TuringException("Could not find target column");
        }

        for (const NodeID val : *col) {
            _targetNodes.insert(val);
        }

        _target.getPort()->consume();
    }

    if (_source.getPort()->hasData()) {
        const Dataframe* sourceDf = _source.getDataframe();
        auto* col = sourceDf->getColumn<ColumnNodeIDs>(_sourceColumn);

        if (!col) {
            throw TuringException("Could not find source column");
        }

        for (const auto val : *col) {
            _sourceNodes.push_back(val);
        }

        _source.getPort()->consume();
    }

    if (!(_source.getPort()->isClosed() &&
        _target.getPort()->isClosed())) {
        finish();
        return;
    }

    const Dataframe* outDf = _out.getDataframe();

    auto* sourceOutputCol = outDf->getColumn<ColumnVector<NodeID>>(_sourceOutputTag);
    if (!sourceOutputCol) {
        throw TuringException("Could not find source output column");
    }

    auto* targetOutputCol = outDf->getColumn<ColumnVector<NodeID>>(_targetOutputTag);
    if (!targetOutputCol) {
        throw TuringException("Could not find target output column");
    }

    auto* distCol = outDf->getColumn<ColumnVector<EdgePropType>>(_distTag);
    if (!distCol) {
        throw TuringException("Could not find distance column");
    }

    auto* pathCol = outDf->getColumn<ColumnVector<Path>>(_pathTag);
    if (!pathCol) {
        throw TuringException("Could not find path column");
    }

    // Run an independent Dijkstra from each source node to find shortest paths to all targets.
    for (const NodeID sourceNode : _sourceNodes) {
        runDijkstra(sourceNode, sourceOutputCol, targetOutputCol, distCol, pathCol);
    }

    _out.getPort()->writeData();
    finish();
}

// Runs Dijkstra's algorithm from a single source node. For each reachable target,
// emits one row with the source identity, target identity, shortest distance, and
// the reconstructed path. The heap and value map are class members to avoid
// repeated allocation; they are cleared at the start of each invocation.
template <SupportedType T>
void MultiSourceShortestPathProcessor<T>::runDijkstra(NodeID sourceNode,
                                                      ColumnVector<NodeID>* sourceOutputCol,
                                                      ColumnVector<NodeID>* targetOutputCol,
                                                      ColumnVector<EdgePropType>* distCol,
                                                      ColumnVector<Path>* pathCol) {
    // Reset state from any previous source invocation.
    _heap = DijkstraHeap<EdgePropType>();
    _heapValueMap.clear();
    _settledTargets.clear();

    _heap.push({sourceNode, NodeID(), EdgeID(), 0});
    _heapValueMap.insert({sourceNode, {NodeID(), EdgeID(), 0}});

    while (!_heap.empty()) {
        const DijkstraNode<EdgePropType> val = _heap.top();
        _heap.pop();

        // Skip stale entries — a node can appear multiple times in the heap
        // when a shorter path is discovered after the initial insertion.
        const auto it = _heapValueMap.find(val.id);
        if (it != _heapValueMap.end() && it->second.distance != val.distance) {
            continue;
        }

        // When a target node is settled, record the result row and reconstruct
        // the path by walking backwards through the predecessor chain.
        if (_targetNodes.contains(val.id) && !_settledTargets.contains(val.id)) {
            _settledTargets.insert(val.id);

            sourceOutputCol->push_back(sourceNode);
            targetOutputCol->push_back(val.id);
            distCol->push_back(val.distance);

            auto& pathVec = pathCol->emplace_back();
            auto lastNode = val.prevNode;
            auto edge = val.edge;
            pathVec.push_back(val.id.getValue());

            while (lastNode.isValid()) {
                pathVec.push_back(edge.getValue());
                pathVec.push_back(lastNode.getValue());

                const auto& pathInfo = _heapValueMap[lastNode];
                lastNode = pathInfo.prevNode;
                edge = pathInfo.edge;
            }

            // Early exit once all targets have been reached.
            if (_settledTargets.size() == _targetNodes.size()) {
                break;
            }
        }

        // Expand the current node: fetch all outgoing edges and their weights.
        _input->clear();
        _input->push_back(val.id);
        _getOutEdgesWriter->reset();
        _getOutEdgesWriter->fill(SIZE_MAX);

        _getPropertiesWriter->reset();
        _getPropertiesWriter->fill(SIZE_MAX);

        for (size_t i = 0; i < _properties->size(); ++i) {

            if constexpr (std::is_signed_v<EdgePropType>) {
                if ((*_properties)[i] < 0) {
                    throw PipelineException("Cannot Do Shortest Path With Negative Weights");
                }
            }

            const auto outputNodeId = (*_outputNodes)[(*_propertyIndices)[i]];
            const auto outputEdgeId = (*_outputEdges)[(*_propertyIndices)[i]];
            const auto dist = val.distance + (*_properties)[i];

            // Relax the edge: update the shortest known distance if this path is better.
            const auto neighborIt = _heapValueMap.find(outputNodeId);
            if (neighborIt == _heapValueMap.end()) {
                _heap.push({outputNodeId, val.id, outputEdgeId, dist});
                _heapValueMap[outputNodeId] = {val.id, outputEdgeId, dist};
            } else if (dist < neighborIt->second.distance) {
                _heap.push({outputNodeId, val.id, outputEdgeId, dist});
                _heapValueMap[outputNodeId] = {val.id, outputEdgeId, dist};
            }
        }
    }
}

namespace db {
template class MultiSourceShortestPathProcessor<types::UInt64>;
template class MultiSourceShortestPathProcessor<types::Int64>;
template class MultiSourceShortestPathProcessor<types::Double>;
}
