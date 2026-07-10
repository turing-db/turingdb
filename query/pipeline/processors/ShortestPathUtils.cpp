#include "ShortestPathUtils.h"

#include "PipelineException.h"
#include "iterators/GetOutEdgesIterator.h"

#include "metadata/PropertyType.h"

using namespace db;

template <SupportedType T>
DijkstraRunner<T>::DijkstraRunner() = default;

template <SupportedType T>
DijkstraRunner<T>::~DijkstraRunner() = default;

template <SupportedType T>
void DijkstraRunner<T>::initialize(ColumnNodeIDs* inputNodes,
                                   ColumnEdgeIDs* outputEdges,
                                   ColumnNodeIDs* outputNodes,
                                   ColumnIndices* outputIndices,
                                   GetOutEdgesChunkWriter* getOutEdgesWriter,
                                   ColumnIndices* propertyIndices,
                                   ColumnVector<EdgePropType>* properties,
                                   GetPropertiesChunkWriter<EdgeID, T>* getPropertiesWriter) {
    _inputNodes = inputNodes;
    _outputEdges = outputEdges;
    _outputNodes = outputNodes;
    _outputIndices = outputIndices;
    _getOutEdgesWriter = getOutEdgesWriter;
    _propertyIndices = propertyIndices;
    _properties = properties;
    _getPropertiesWriter = getPropertiesWriter;
}

template <SupportedType T>
void DijkstraRunner<T>::run(const DijkstraHeap<EdgePropType>& initialHeap,
                            const DijkstraValueMap<EdgePropType>& initialValues,
                            const std::unordered_set<NodeID>& targetNodes,
                            bool stopAtFirst,
                            const SubpathCache<EdgePropType>* cache) {
    _heap = initialHeap;
    _heapValueMap = initialValues;
    _settledTargets.clear();
    _results.clear();
    _pendingResults.clear();

    while (!_heap.empty()) {
        const DijkstraNode<EdgePropType> val = _heap.top();
        _heap.pop();

        const auto it = _heapValueMap.find(val.id);
        if (it != _heapValueMap.end() && it->second.distance != val.distance) {
            continue;
        }

        // Finalize pending cache-hit results whose distance cannot be beaten
        // by any future path (all remaining nodes have distance >= val.distance).
        for (auto pendingIt = _pendingResults.begin(); pendingIt != _pendingResults.end(); ) {
            if (pendingIt->second.distance <= val.distance) {
                _settledTargets.insert(pendingIt->second.targetNode);
                _results.push_back(pendingIt->second);
                pendingIt = _pendingResults.erase(pendingIt);
            } else {
                ++pendingIt;
            }
        }

        if (targetNodes.contains(val.id) && !_settledTargets.contains(val.id)) {
            _settledTargets.insert(val.id);
            _pendingResults.erase(val.id);

            DijkstraResult<EdgePropType> result;
            result.targetNode = val.id;
            result.distance = val.distance;
            reconstructPath(val, result.path);
            _results.push_back(result);

            if (stopAtFirst) {
                break;
            }

            if (_settledTargets.size() == targetNodes.size()) {
                break;
            }
        }

        // Consult the subpath cache: if this settled node has known shortest
        // paths to unsettled targets, record them as pending results.
        if (cache) {
            const auto cacheIt = cache->find(val.id);
            if (cacheIt != cache->end()) {
                for (const SubpathCacheEntry<EdgePropType>& entry : cacheIt->second) {
                    if (!targetNodes.contains(entry.targetNode)) {
                        continue;
                    }
                    if (_settledTargets.contains(entry.targetNode)) {
                        continue;
                    }

                    const EdgePropType candidateDistance = val.distance + entry.distance;

                    const auto existingIt = _pendingResults.find(entry.targetNode);
                    if (existingIt != _pendingResults.end() &&
                        existingIt->second.distance <= candidateDistance) {
                        continue;
                    }

                    DijkstraResult<EdgePropType> pending;
                    pending.targetNode = entry.targetNode;
                    pending.distance = candidateDistance;
                    buildCacheHitPath(val, entry, pending.path);
                    _pendingResults[entry.targetNode] = pending;
                }
            }
        }

        expandNode(val);
    }

    // Finalize any remaining pending results (heap exhausted, no shorter path exists).
    for (auto& [targetNode, pending] : _pendingResults) {
        if (!_settledTargets.contains(targetNode)) {
            _settledTargets.insert(targetNode);
            _results.push_back(pending);
        }
    }
    _pendingResults.clear();
}

template <SupportedType T>
void DijkstraRunner<T>::expandNode(const DijkstraNode<EdgePropType>& node) {
    _inputNodes->clear();
    _inputNodes->push_back(node.id);
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
        const auto dist = node.distance + (*_properties)[i];

        const auto it = _heapValueMap.find(outputNodeId);
        if (it == _heapValueMap.end()) {
            _heap.push({outputNodeId, node.id, outputEdgeId, dist});
            _heapValueMap[outputNodeId] = {node.id, outputEdgeId, dist};
        } else if (dist < it->second.distance) {
            _heap.push({outputNodeId, node.id, outputEdgeId, dist});
            _heapValueMap[outputNodeId] = {node.id, outputEdgeId, dist};
        }
    }
}

template <SupportedType T>
void DijkstraRunner<T>::buildCacheHitPath(const DijkstraNode<EdgePropType>& settledNode,
                                          const SubpathCacheEntry<EdgePropType>& cacheEntry,
                                          Path& outputPath) {
    // Start with the cached path suffix: [target, edge, ..., edge, settledNode]
    outputPath = cacheEntry.pathSuffix;

    // Append the predecessor chain from settledNode back to the source.
    auto lastNode = settledNode.prevNode;
    auto edge = settledNode.edge;
    while (lastNode.isValid()) {
        outputPath.push_back(edge.getValue());
        outputPath.push_back(lastNode.getValue());

        const auto& pathInfo = _heapValueMap[lastNode];
        lastNode = pathInfo.prevNode;
        edge = pathInfo.edge;
    }
}

template <SupportedType T>
void DijkstraRunner<T>::reconstructPath(const DijkstraNode<EdgePropType>& settledNode,
                                        Path& outputPath) {
    auto lastNode = settledNode.prevNode;
    auto edge = settledNode.edge;
    outputPath.push_back(settledNode.id.getValue());

    while (lastNode.isValid()) {
        outputPath.push_back(edge.getValue());
        outputPath.push_back(lastNode.getValue());

        const auto& pathInfo = _heapValueMap[lastNode];
        lastNode = pathInfo.prevNode;
        edge = pathInfo.edge;
    }
}

namespace db {
template class DijkstraRunner<types::UInt64>;
template class DijkstraRunner<types::Int64>;
template class DijkstraRunner<types::Double>;
}
