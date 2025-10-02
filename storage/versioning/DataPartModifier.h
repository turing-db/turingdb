#pragma once

#include <span>

#include "ArcManager.h"
#include "metadata/SupportedType.h"
#include "writers/DataPartBuilder.h"

namespace db {

class DataPart;

class DataPartModifier {
public:
    DataPartModifier(const WeakArc<DataPart> oldDP, DataPartBuilder& builder,
                         std::span<NodeID> nodesToDelete, std::span<EdgeID> edgesToDelete)
        : _oldDP(oldDP),
        _builder(&builder),
        _nodesToDelete(nodesToDelete),
        _edgesToDelete(edgesToDelete)
    {
    }
    
    DataPartModifier(const DataPartModifier&) = delete;
    DataPartModifier& operator=(const DataPartModifier&) = delete;
    DataPartModifier(DataPartModifier&&) = delete;
    DataPartModifier& operator=(DataPartModifier&&) = delete;

    ~DataPartModifier() = default;

    DataPartBuilder& builder() { return *_builder; }

    void applyModifications();

private:
    using EdgeIDToRecordMap = std::unordered_map<EdgeID, EdgeRecord>;

    const WeakArc<DataPart> _oldDP;
    DataPartBuilder* _builder;

    std::span<NodeID> _nodesToDelete;
    std::span<EdgeID> _edgesToDelete;

    // The only nodes (edges) whose ID changes are those which have an ID greater than a
    // deleted node (edge), as whenever a node (edge) is deleted, all subsequent nodes
    // (edges) need to have their ID reduced by 1 to fill the gap left by the deleted
    // node (edge). Thus, the new ID of a node (edge), x, is equal to the number of
    // deleted nodes (edges) in this datapart which have an ID smaller than x.
    inline NodeID nodeIDMapping(NodeID x) {
        if (std::ranges::binary_search(_nodesToDelete, x)) [[unlikely]] {
            std::string err =
                fmt::format("Attempted to get mapped ID of deleted node: {}.", x);
            throw TuringException(std::move(err));
        }
        // lower_bound O(logn) (binary search); distance is O(1)
        auto smallerNodesIt = std::ranges::lower_bound(_nodesToDelete, x);
        size_t numSmallerNodes = std::distance(_nodesToDelete.begin(), smallerNodesIt);
        return x - numSmallerNodes;
    }
    inline EdgeID edgeIDMapping(EdgeID x) {
        if (std::ranges::binary_search(_edgesToDelete, x)) [[unlikely]] {
            std::string err =
                fmt::format("Attempted to get mapped ID of deleted edge: {}.", x);
            throw TuringException(std::move(err));
        }
        // lower_bound O(logn) (binary search); distance is O(1)
        auto smallerEdgesIt = std::ranges::lower_bound(_edgesToDelete, x);
        size_t numSmallerEdges = std::distance(_edgesToDelete.begin(), smallerEdgesIt);
        return x - numSmallerEdges;
    }

    template <SupportedType T>
    void copyNodeProps(const PropertyManager::PropertyContainerReferences& props);
    template <SupportedType T>
    void copyEdgeProps(const PropertyManager::PropertyContainerReferences& props,
                                     const EdgeIDToRecordMap& edgeMap);
};

template <SupportedType T>
void DataPartModifier::copyNodeProps(const PropertyManager::PropertyContainerReferences& props) {
    for (const auto& [propID, propContainer] : props) {
        const TypedPropertyContainer<T>& container = propContainer->template cast<T>();
        for (const auto [entityID, propValue] : container.zipped()) {
            const NodeID oldNodeID = entityID.getValue();
            if (std::ranges::binary_search(_nodesToDelete, oldNodeID)) {
                continue;
            }
            const NodeID newNodeID = nodeIDMapping(oldNodeID);
            _builder->addNodeProperty<T>(newNodeID, propID, propValue);
        }
    }
}

template <SupportedType T>
void DataPartModifier::copyEdgeProps(const PropertyManager::PropertyContainerReferences& props,
                                     const EdgeIDToRecordMap& edgeMap) {
    for (const auto& [propID, propContainer] : props) {
        TypedPropertyContainer<T>& container = propContainer->template cast<T>();

        for (const auto [entityID, propValue] : container.zipped()) {
            const EdgeID oldEdgeID = entityID.getValue();

            if (std::ranges::binary_search(_edgesToDelete, oldEdgeID)) {
                continue;
            }
            const EdgeRecord& record = edgeMap.at(oldEdgeID);

            _builder->addEdgeProperty<T>(record, propID, propValue);
        }
    }
}

}
