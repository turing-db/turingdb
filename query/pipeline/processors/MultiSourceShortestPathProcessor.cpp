#include "MultiSourceShortestPathProcessor.h"

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

    _runner.initialize(_input, _outputEdges, _outputNodes, _outputIndices,
                       _getOutEdgesWriter.get(),
                       _propertyIndices, _properties, _getPropertiesWriter.get());

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

    SubpathCache<EdgePropType> cache;

    for (const NodeID sourceNode : _sourceNodes) {
        std::unordered_set<NodeID> remainingTargets = _targetNodes;

        // Use cached subpaths: if this source appeared as an intermediate
        // node in a previous run's shortest path, we already know its
        // optimal paths to those targets.
        const auto cacheIt = cache.find(sourceNode);
        if (cacheIt != cache.end()) {
            for (const SubpathCacheEntry<EdgePropType>& entry : cacheIt->second) {
                if (!remainingTargets.contains(entry.targetNode)) {
                    continue;
                }

                sourceOutputCol->push_back(sourceNode);
                targetOutputCol->push_back(entry.targetNode);
                distCol->push_back(entry.distance);
                auto& pathVec = pathCol->emplace_back();
                pathVec = entry.pathSuffix;
                remainingTargets.erase(entry.targetNode);
            }
        }

        if (!remainingTargets.empty()) {
            DijkstraHeap<EdgePropType> heap;
            DijkstraValueMap<EdgePropType> valueMap;
            heap.push({sourceNode, NodeID(), EdgeID(), 0});
            valueMap.insert({sourceNode, {NodeID(), EdgeID(), 0}});

            _runner.run(heap, valueMap, remainingTargets, false, &cache);

            for (const DijkstraResult<EdgePropType>& result : _runner.results()) {
                sourceOutputCol->push_back(sourceNode);
                targetOutputCol->push_back(result.targetNode);
                distCol->push_back(result.distance);
                auto& pathVec = pathCol->emplace_back();
                pathVec = result.path;
            }
        }

        // Populate cache from the run's results: for each settled target,
        // walk the path and cache subpaths from every intermediate node.
        for (const DijkstraResult<EdgePropType>& result : _runner.results()) {
            const auto& path = result.path;
            const auto& valueMap = _runner.getValueMap();

            // path = [target, edge, node, edge, node, ..., source]
            // Nodes are at even indices; skip index 0 (target) and the last
            // node (source, which is the current sourceNode).
            for (size_t i = 2; i < path.size(); i += 2) {
                const NodeID intermediateNode(path[i].getValue());
                if (intermediateNode == sourceNode) {
                    continue;
                }

                const auto valueIt = valueMap.find(intermediateNode);
                if (valueIt == valueMap.end()) {
                    continue;
                }

                const EdgePropType distanceToTarget = result.distance - valueIt->second.distance;
                const Path pathSuffix(path.begin(), path.begin() + i + 1);

                bool alreadyCached = false;
                const auto existingIt = cache.find(intermediateNode);
                if (existingIt != cache.end()) {
                    for (const SubpathCacheEntry<EdgePropType>& existing : existingIt->second) {
                        if (existing.targetNode == result.targetNode) {
                            alreadyCached = true;
                            break;
                        }
                    }
                }

                if (!alreadyCached) {
                    cache[intermediateNode].push_back({result.targetNode,
                                                       distanceToTarget,
                                                       pathSuffix});
                }
            }
        }
    }

    _out.getPort()->writeData();
    finish();
}

namespace db {
template class MultiSourceShortestPathProcessor<types::UInt64>;
template class MultiSourceShortestPathProcessor<types::Int64>;
template class MultiSourceShortestPathProcessor<types::Double>;
}
