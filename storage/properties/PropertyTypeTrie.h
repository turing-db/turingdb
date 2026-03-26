#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <span>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "ID.h"
#include "PropertyTypeSet.h"

#include "BioAssert.h"
#include "Profiler.h"

namespace db {

class PropertyTypeTrie {
    static constexpr uint16_t kNoNode = std::numeric_limits<uint16_t>::max();

public:
    void build(const std::unordered_map<PropertyTypeID, std::span<const EntityID>>& typesToEntities,
               EntityID firstCoreEntityID,
               size_t coreEntityCount) {
        const Profile profile("PropertyTypeTrie::build");

        _firstCoreEntityID = firstCoreEntityID;
        _coreEntityCount = coreEntityCount;

        // Step 1. Sort PropertyTypeIDs.
        //         Sorted order guarantees that two entities with the same set
        //         follow the exact same trie path and land on the same leaf node.
        std::vector<PropertyTypeID> sortedTypes;
        {
            const Profile profile("PropertyTypeTrie::build <sort types>");
            sortedTypes.reserve(typesToEntities.size());

            for (const auto& [ptId, entities] : typesToEntities) {
                sortedTypes.push_back(ptId);
            }

            std::sort(sortedTypes.begin(), sortedTypes.end());
        }

        // Step 2. Create the root node (empty PropertyTypeSet) and register every
        //         entity that has at least one property type, mapping it to the root (index 0).
        //         Patch entities (id < firstCoreEntityID) go into a sorted map;
        //         core entities go into a vector indexed by (id - firstCoreEntityID).
        _nodes.clear();
        _nodes.emplace_back(); // root is always index 0
        _coreEntityToNode.assign(coreEntityCount, kNoNode);

        for (const auto& [ptId, entities] : typesToEntities) {
            for (const EntityID e : entities) {
                if (e < firstCoreEntityID) {
                    _patchEntityToNode.emplace(e, 0);
                } else {
                    const size_t idx = (e - firstCoreEntityID).getValue();
                    if (idx < coreEntityCount && _coreEntityToNode[idx] == kNoNode) {
                        _coreEntityToNode[idx] = 0;
                    }
                }
            }
        }

        // Step 3. For each PT in sorted order, merge-walk patch entities then core
        //         entities with the PT's span, routing each entity to the appropriate
        //         child. Children are created lazily on first use.
        //
        //         Because patch IDs < firstCoreEntityID <= core IDs, and spans are
        //         sorted, a single spanIt carried from the patch loop into the core
        //         loop produces a correct sorted merge.
        for (const PropertyTypeID ptId : sortedTypes) {
            const std::span<const EntityID> span = typesToEntities.at(ptId);
            auto spanIt = span.begin();

            for (auto& [e, nodeIdx] : _patchEntityToNode) {
                while (spanIt != span.end() && *spanIt < e) {
                    ++spanIt;
                }

                const uint16_t curIdx = nodeIdx;
                const bool hasIt = (spanIt != span.end() && *spanIt == e);

                if (hasIt) {
                    if (_nodes[curIdx].hasPt == kNoNode) {
                        // Copy before allocNode(): push_back may reallocate _nodes.
                        const PropertyTypeSet ptsCopy = _nodes[curIdx].propertyTypeSet;
                        const uint16_t newIdx = allocNode();
                        _nodes[newIdx].propertyTypeSet = ptsCopy;
                        _nodes[newIdx].propertyTypeSet.add(ptId); // sorted: ptId > all previous
                        _nodes[curIdx].hasPt = newIdx;
                    }

                    nodeIdx = _nodes[curIdx].hasPt;
                    ++spanIt;
                } else {
                    if (_nodes[curIdx].lacksPt == kNoNode) {
                        const PropertyTypeSet ptsCopy = _nodes[curIdx].propertyTypeSet;
                        const uint16_t newIdx = allocNode();
                        _nodes[newIdx].propertyTypeSet = ptsCopy;
                        _nodes[curIdx].lacksPt = newIdx;
                    }

                    nodeIdx = _nodes[curIdx].lacksPt;
                }
            }

            for (size_t idx = 0; idx < coreEntityCount; ++idx) {
                if (_coreEntityToNode[idx] == kNoNode) {
                    continue;
                }

                const EntityID e = firstCoreEntityID + idx;

                while (spanIt != span.end() && *spanIt < e) {
                    ++spanIt;
                }

                const uint16_t curIdx = _coreEntityToNode[idx];
                const bool hasIt = (spanIt != span.end() && *spanIt == e);

                if (hasIt) {
                    if (_nodes[curIdx].hasPt == kNoNode) {
                        const PropertyTypeSet ptsCopy = _nodes[curIdx].propertyTypeSet;
                        const uint16_t newIdx = allocNode();
                        _nodes[newIdx].propertyTypeSet = ptsCopy;
                        _nodes[newIdx].propertyTypeSet.add(ptId); // sorted: ptId > all previous
                        _nodes[curIdx].hasPt = newIdx;
                    }
                    _coreEntityToNode[idx] = _nodes[curIdx].hasPt;
                    ++spanIt;
                } else {
                    if (_nodes[curIdx].lacksPt == kNoNode) {
                        const PropertyTypeSet ptsCopy = _nodes[curIdx].propertyTypeSet;
                        const uint16_t newIdx = allocNode();
                        _nodes[newIdx].propertyTypeSet = ptsCopy;
                        _nodes[curIdx].lacksPt = newIdx;
                    }
                    _coreEntityToNode[idx] = _nodes[curIdx].lacksPt;
                }
            }
        }
    }

    /** @brief Returns the PropertyTypeSet for the given entity. O(1).
     * The returned reference is stable for the lifetime of this object.
     *
     * @param entityId The entity ID.
     * @return The PropertyTypeSet.
     * */
    const PropertyTypeSet& getPropertyTypeSet(EntityID entityId) const {
        if (entityId < _firstCoreEntityID) {
            return _nodes[_patchEntityToNode.at(entityId)].propertyTypeSet;
        }

        const size_t idx = (entityId - _firstCoreEntityID).getValue();
        if (idx >= _coreEntityCount || _coreEntityToNode[idx] == kNoNode) {
            throw std::out_of_range("Entity not found in PropertyTypeTrie");
        }

        return _nodes[_coreEntityToNode[idx]].propertyTypeSet;
    }

    /** @brief Returns the PropertyTypeSet for the given entity, or nullptr if
     * the entity is not tracked (i.e. has no properties). O(1).
     *
     * @param entityId The entity ID.
     * @return The PropertyTypeSet, or nullptr if the entity is not tracked.
     * */
    const PropertyTypeSet* tryGetPropertyTypeSet(EntityID entityId) const {
        if (entityId < _firstCoreEntityID) {
            const auto it = _patchEntityToNode.find(entityId);
            if (it == _patchEntityToNode.end()) {
                return nullptr;
            }

            return &_nodes[it->second].propertyTypeSet;
        }

        const size_t idx = (entityId - _firstCoreEntityID).getValue();
        if (idx >= _coreEntityCount || _coreEntityToNode[idx] == kNoNode) {
            return nullptr;
        }

        return &_nodes[_coreEntityToNode[idx]].propertyTypeSet;
    }

    /** @brief Returns true if the given entity has the given property type. O(log P).
     *
     * @param entityId The entity ID.
     * @param ptId The property type ID.
     * @return True if the entity has the property type, false otherwise.
     * */
    bool hasPropertyType(EntityID entityId, PropertyTypeID ptId) const {
        if (entityId < _firstCoreEntityID) {
            return _nodes[_patchEntityToNode.at(entityId)].propertyTypeSet.contains(ptId);
        }

        const size_t idx = (entityId - _firstCoreEntityID).getValue();
        if (idx >= _coreEntityCount || _coreEntityToNode[idx] == kNoNode) {
            throw std::out_of_range("Entity not found in PropertyTypeTrie");
        }

        return _nodes[_coreEntityToNode[idx]].propertyTypeSet.contains(ptId);
    }

    /** @brief Returns the number of entities registered (i.e. with at least one property type).
     *
     * @return The number of entities registered.
     * */
    size_t entityCount() const {
        size_t coreCount = 0;

        for (const uint16_t nodeIdx : _coreEntityToNode) {
            if (nodeIdx != kNoNode) {
                ++coreCount;
            }
        }

        return _patchEntityToNode.size() + coreCount;
    }

    /** @brief Remaps all PropertyTypeIDs stored in every trie node in-place.
     *  The trie structure (entity-to-node mapping) is unchanged; only the PT ID
     *  values inside each node's PropertyTypeSet are updated.
     *
     * @param remapper A callable (PropertyTypeID) -> PropertyTypeID applied to every stored ID.
     * */
    template <typename Remapper>
    void rebasePropertyTypes(const Remapper& remapper) {
        for (TrieNode& node : _nodes) {
            for (PropertyTypeID& id : node.propertyTypeSet.get()) {
                id = remapper(id);
            }
        }
    }

    /** @brief Remaps all EntityIDs in the entity-to-node mappings in-place.
     *  The trie node structure is unchanged; only the keys in _patchEntityToNode
     *  and the _firstCoreEntityID base are updated.
     *
     * @param remapper A callable (EntityID) -> EntityID applied to every stored entity ID.
     * */
    template <typename Remapper>
    void rebaseEntityIDs(const Remapper& remapper) {
        std::map<EntityID, uint16_t> newMap;
        for (auto& [e, nodeIdx] : _patchEntityToNode) {
            newMap[remapper(e)] = nodeIdx;
        }

        _patchEntityToNode = std::move(newMap);
        _firstCoreEntityID = remapper(_firstCoreEntityID);
    }

private:
    struct TrieNode {
        PropertyTypeSet propertyTypeSet;
        uint16_t lacksPt {kNoNode};
        uint16_t hasPt {kNoNode};
    };

    uint16_t allocNode() {
        bioassert(_nodes.size() < kNoNode, "PropertyTypeTrie node count exceeds uint16_t capacity");
        _nodes.push_back(TrieNode{});
        return static_cast<uint16_t>(_nodes.size() - 1);
    }

    std::vector<TrieNode> _nodes;
    std::map<EntityID, uint16_t> _patchEntityToNode;
    std::vector<uint16_t> _coreEntityToNode;
    EntityID _firstCoreEntityID {0};
    size_t _coreEntityCount {0};
};

} // namespace db
