#include "ShortestPathProcessor.h"

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
ShortestPathProcessor<T>::ShortestPathProcessor(LocalMemory* mem,
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
ShortestPathProcessor<T>* ShortestPathProcessor<T>::create(PipelineV2* pipeline,
                                                           LocalMemory* mem,
                                                           ColumnTag sourceTag,
                                                           ColumnTag targetTag,
                                                           const PropertyType& edgeType) {
    auto* processor = new ShortestPathProcessor(mem,
                                                sourceTag,
                                                targetTag,
                                                edgeType);

    {
        PipelineInputPort* sourceInputPort = PipelineInputPort::create(pipeline, processor);
        processor->_source.setPort(sourceInputPort);
        processor->addInput(sourceInputPort);
        // For the inputs - they can fill up independently so neither port needs data
        // for the processor to run
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
void ShortestPathProcessor<T>::prepare(ExecutionContext* ctxt) {
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
void ShortestPathProcessor<T>::reset() {
    markAsReset();
}

template <SupportedType T>
void ShortestPathProcessor<T>::execute() {
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
            _heap.push({val, NodeID(), EdgeID(), 0});
            _heapValueMap.insert({
                val, {NodeID(), EdgeID(), 0}
            });
        }

        _source.getPort()->consume();
    }

    // execute the algorithm only once we have all the data
    if (!(_source.getPort()->isClosed() &&
        _target.getPort()->isClosed())) {
        finish();
        return;
    }
    while (!_heap.empty()) {
        const DjistrakaNode<EdgePropType> val = _heap.top();

        if (_targetNodes.contains(val.id)) {
            // target found
            break;
        }

        _heap.pop();

        // We only need to confirm if the distance value is stale
        // as we only add a new value to the heap when we find a shorter
        // path to a node.
        const auto it = _heapValueMap.find(val.id);
        if (it != _heapValueMap.end() && it->second.distance != val.distance) {
            // remove stale value
            continue;
        }

        // Set up the input columns and iterators
        _input->clear();
        _input->push_back(val.id);
        _getOutEdgesWriter->reset();
        // We set the chunk size to SIZE_MAX so that we expand the iterators
        // completely in 1 chunk
        _getOutEdgesWriter->fill(SIZE_MAX);

        _getPropertiesWriter->reset();
        _getPropertiesWriter->fill(SIZE_MAX);

        // loop over all the edge properties
        for (size_t i = 0; i < _properties->size(); ++i) {

            if constexpr (std::is_signed_v<EdgePropType>) {
                if ((*_properties)[i] < 0) {
                    throw PipelineException("Cannot Do Shortest Path With Negative Weights");
                }
            }

            // Using the indices we for each edge we have the:
            // 1.Target Node
            // 2.EdgeID
            // 3.Edge Property
            const auto outputNodeId = (*_outputNodes)[(*_propertyIndices)[i]];
            const auto outputEdgeId = (*_outputEdges)[(*_propertyIndices)[i]];
            const auto dist = val.distance + (*_properties)[i];

            const auto it = _heapValueMap.find(outputNodeId);
            if (it == _heapValueMap.end()) {
                // If the node is not in the _heapValueMap
                // then it is an unexplored node so we add it into heap
                // directly
                _heap.push({outputNodeId, val.id, outputEdgeId, dist});
                _heapValueMap[outputNodeId] = {val.id, outputEdgeId, dist};
            } else {
                // If we have come across this nodeID before we check if the new path
                // to the node is shorter than the current shortest path for the node
                // If so we replace it by pushing the new djistraka node into the heap
                // and changing the _heapValueMap value to match the new shortest distance.
                //
                // The old path's node will still exist in the heap but if we ever pop
                // it from the heap we can compare it to the latest value in the _heapValueMap
                // to invalidate it.
                if (dist < it->second.distance) {
                    _heap.push({outputNodeId, val.id, outputEdgeId, dist});
                    _heapValueMap[outputNodeId] = {val.id, outputEdgeId, dist};
                }
            }
        }
    }

    // Once we are out of the while loop if there are no values in the heap that means
    // no path was found between the source and target sets.

    if (_heap.empty()) {
        _out.getPort()->writeData();
        finish();
        return;
    }

    // After the loop the top value in the heap is the target node with the shortest
    // distance from the source set.

    const Dataframe* outDf = _out.getDataframe();

    ColumnVector<EdgePropType>* distCol = outDf->getColumn<ColumnVector<EdgePropType>>(_distTag);
    if (!distCol) {
        throw TuringException("Could not find distance column");
    }

    ColumnVector<Path>* pathCol = outDf->getColumn<ColumnVector<Path>>(_pathTag);
    if (!pathCol) {
        throw TuringException("Could not find path column");
    }

    // Get the total length of the path
    distCol->push_back(_heap.top().distance);
    auto& pathVec = pathCol->emplace_back();

    // In the heap all the (non-stale) values represent the shortest path to that
    // nodeID from the source set. So we can find the shortet path by following the
    // previousNode back from the target node that we have found.
    auto lastNode = _heap.top().prevNode;
    auto edge = _heap.top().edge;
    pathVec.push_back(_heap.top().id.getValue());

    while (lastNode.isValid()) {
        pathVec.push_back(edge.getValue());
        pathVec.push_back(lastNode.getValue());

        const auto& pathInfo = _heapValueMap[lastNode];
        lastNode = pathInfo.prevNode;
        edge = pathInfo.edge;
    }
    _out.getPort()->writeData();
    finish();
}

namespace db {
template class ShortestPathProcessor<types::UInt64>;
template class ShortestPathProcessor<types::Int64>;
template class ShortestPathProcessor<types::Double>;
}
