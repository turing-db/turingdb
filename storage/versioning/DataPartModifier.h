#pragma once

#include <limits>
#include <span>
#include <unordered_map>

#include "ArcManager.h"
#include "BioAssert.h"
#include "metadata/SupportedType.h"
#include "writers/DataPartBuilder.h"

namespace db {

class DataPart;

class DataPartModifier {
public:
    DataPartModifier(const WeakArc<DataPart> oldDP,
                     DataPartBuilder& builder,
                     const CommitWriteBuffer& wb)
        : _oldDP(oldDP),
          _builder(&builder),
          _writeBuffer(wb)
   {
   }

    DataPartModifier(const DataPartModifier&) = delete;
    DataPartModifier& operator=(const DataPartModifier&) = delete;
    DataPartModifier(DataPartModifier&&) = delete;
    DataPartModifier& operator=(DataPartModifier&&) = delete;

    ~DataPartModifier() = default;

    DataPartBuilder& builder() { return *_builder; }

    void applyModifications(size_t index);

    /**
    * @brief Returns the DataPart index that @param node exists in.
    */
    size_t getDataPartIndex(NodeID node);

private:
    // Maps old EdgeIDs to new EdgeRecords
    using EdgeIDToRecordMap = std::unordered_map<EdgeID, EdgeRecord>;
    // Memoisation of nodes to the datapart they are contained in
    using NodeToDataPartMap = std::unordered_map<NodeID, size_t>;

    // Old DP to apply modifications to
    const WeakArc<DataPart> _oldDP;
    // New builder which, when built, will be _oldDP with deletions applied
    DataPartBuilder* _builder;
    // Write buffer which specifies what nodes/edges to be deleted
    const CommitWriteBuffer& _writeBuffer;
    // The index of the datapart which is being modified in its parent commit
    size_t _dpIndex {std::numeric_limits<size_t>::max()};

    NodeToDataPartMap _nodeToDPMap;

    // Nodes to delete from  datapart at index @ref _dpIndex in parent commit
    std::span<NodeID> _nodesToDelete;
    // Edges to delete from  datapart at index @ref _dpIndex in parent commit
    std::span<EdgeID> _edgesToDelete;

    // The only nodes (edges) whose ID changes are those which have an ID greater than a
    // deleted node (edge), as whenever a node (edge) is deleted, all subsequent nodes
    // (edges) need to have their ID reduced by 1 to fill the gap left by the deleted
    // node (edge). Thus, the new ID of a node (edge), x, is equal to the number of
    // deleted nodes (edges) in this datapart which have an ID smaller than x.
    inline NodeID nodeIDMapping(NodeID x) {
        size_t dpIndex = getDataPartIndex(x);
        std::span<NodeID> relevantDeletedNodes =
            _writeBuffer.deletedNodesFromDataPart(dpIndex);

        if (std::ranges::binary_search(relevantDeletedNodes, x)) [[unlikely]] {
            panic("Attempted to get mapped id of deleted edge: {}.", x);
        }
        // lower_bound O(logn) (binary search); distance is O(1)
        auto smallerNodesIt = std::ranges::lower_bound(relevantDeletedNodes, x);
        size_t numSmallerNodes = std::distance(relevantDeletedNodes.begin(), smallerNodesIt);
        return x - numSmallerNodes;
    }

    inline EdgeID edgeIDMapping(EdgeID x) {
        if (std::ranges::binary_search(_edgesToDelete, x)) [[unlikely]] {
            panic("Attempted to get mapped id of deleted edge: {}.", x);
        }
        // lower_bound O(logn) (binary search); distance is O(1)
        auto smallerEdgesIt = std::ranges::lower_bound(_edgesToDelete, x);
        size_t numSmallerEdges = std::distance(_edgesToDelete.begin(), smallerEdgesIt);
        return x - numSmallerEdges;
    }

    /**
     * @brief Given a map of [property ID, property container<T>], iterates through the
     * property container, adding properties to @ref _builder for nodes which do not
     * appear in @ref _nodesToDelete.
     */
    template <SupportedType T>
    void copyNodeProps(const PropertyManager::PropertyContainerReferences& props);

    /**
     * @brief Given a map of [property ID, property container<T>], iterates through the
     * property container, adding properties to @ref _builder for edges which do not
     * appear in @ref _edgesToDelete.
     */
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

            bioassert(std::ranges::is_sorted(_edgesToDelete));

            if (std::ranges::binary_search(_edgesToDelete, oldEdgeID)) {
                continue;
            }
            const EdgeRecord& record = edgeMap.at(oldEdgeID);

            _builder->addEdgeProperty<T>(record, propID, propValue);
        }
    }
}

}
