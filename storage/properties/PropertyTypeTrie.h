#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <span>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "ID.h"
#include "PropertyTypeSet.h"

#include "Profiler.h"

namespace db {

class PropertyTypeTrie {
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
        //         entity that has at least one property type, mapping it to _root.
        //         Patch entities (id < firstCoreEntityID) go into a sorted map;
        //         core entities go into a vector indexed by (id - firstCoreEntityID).
        _root = std::make_unique<TrieNode>();
        _coreEntityToNode.resize(coreEntityCount, nullptr);

        for (const auto& [ptId, entities] : typesToEntities) {
            for (const EntityID e : entities) {
                if (e < firstCoreEntityID) {
                    _patchEntityToNode.emplace(e, _root.get());
                } else {
                    const size_t idx = (e - firstCoreEntityID).getValue();
                    if (idx < coreEntityCount && _coreEntityToNode[idx] == nullptr) {
                        _coreEntityToNode[idx] = _root.get();
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

            for (auto& [e, nodePtr] : _patchEntityToNode) {
                while (spanIt != span.end() && *spanIt < e) {
                    ++spanIt;
                }

                TrieNode* cur = nodePtr;
                const bool hasIt = (spanIt != span.end() && *spanIt == e);

                if (hasIt) {
                    if (!cur->hasPt) {
                        cur->hasPt = std::make_unique<TrieNode>();
                        cur->hasPt->propertyTypeSet = cur->propertyTypeSet;
                        cur->hasPt->propertyTypeSet.add(ptId); // sorted: ptId > all previous
                    }

                    nodePtr = cur->hasPt.get();
                    ++spanIt;
                } else {
                    if (!cur->lacksPt) {
                        cur->lacksPt = std::make_unique<TrieNode>();
                        cur->lacksPt->propertyTypeSet = cur->propertyTypeSet;
                    }

                    nodePtr = cur->lacksPt.get();
                }
            }

            for (size_t idx = 0; idx < coreEntityCount; ++idx) {
                if (_coreEntityToNode[idx] == nullptr) {
                    continue;
                }

                const EntityID e = firstCoreEntityID + idx;

                while (spanIt != span.end() && *spanIt < e) {
                    ++spanIt;
                }

                TrieNode* cur = _coreEntityToNode[idx];
                const bool hasIt = (spanIt != span.end() && *spanIt == e);

                if (hasIt) {
                    if (!cur->hasPt) {
                        cur->hasPt = std::make_unique<TrieNode>();
                        cur->hasPt->propertyTypeSet = cur->propertyTypeSet;
                        cur->hasPt->propertyTypeSet.add(ptId); // sorted: ptId > all previous
                    }
                    _coreEntityToNode[idx] = cur->hasPt.get();
                    ++spanIt;
                } else {
                    if (!cur->lacksPt) {
                        cur->lacksPt = std::make_unique<TrieNode>();
                        cur->lacksPt->propertyTypeSet = cur->propertyTypeSet;
                    }
                    _coreEntityToNode[idx] = cur->lacksPt.get();
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
            return _patchEntityToNode.at(entityId)->propertyTypeSet;
        }

        const size_t idx = (entityId - _firstCoreEntityID).getValue();
        if (idx >= _coreEntityCount || _coreEntityToNode[idx] == nullptr) {
            throw std::out_of_range("Entity not found in PropertyTypeTrie");
        }

        return _coreEntityToNode[idx]->propertyTypeSet;
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

            return &it->second->propertyTypeSet;
        }

        const size_t idx = (entityId - _firstCoreEntityID).getValue();
        if (idx >= _coreEntityCount || _coreEntityToNode[idx] == nullptr) {
            return nullptr;
        }

        return &_coreEntityToNode[idx]->propertyTypeSet;
    }

    /** @brief Returns true if the given entity has the given property type. O(log P).
     *
     * @param entityId The entity ID.
     * @param ptId The property type ID.
     * @return True if the entity has the property type, false otherwise.
     * */
    bool hasPropertyType(EntityID entityId, PropertyTypeID ptId) const {
        if (entityId < _firstCoreEntityID) {
            return _patchEntityToNode.at(entityId)->propertyTypeSet.contains(ptId);
        }

        const size_t idx = (entityId - _firstCoreEntityID).getValue();
        if (idx >= _coreEntityCount || _coreEntityToNode[idx] == nullptr) {
            throw std::out_of_range("Entity not found in PropertyTypeTrie");
        }

        return _coreEntityToNode[idx]->propertyTypeSet.contains(ptId);
    }

    /** @brief Returns the number of entities registered (i.e. with at least one property type).
     *
     * @return The number of entities registered.
     * */
    size_t entityCount() const {
        size_t coreCount = 0;

        for (const TrieNode* node : _coreEntityToNode) {
            if (node != nullptr) {
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
        rebaseNodePropertyTypes(_root.get(), remapper);
    }

private:
    struct TrieNode {
        PropertyTypeSet propertyTypeSet;
        std::unique_ptr<TrieNode> lacksPt;
        std::unique_ptr<TrieNode> hasPt;
    };

    template <typename Remapper>
    static void rebaseNodePropertyTypes(TrieNode* node, const Remapper& remapper) {
        for (PropertyTypeID& id : node->propertyTypeSet.get()) {
            id = remapper(id);
        }

        if (node->hasPt) {
            rebaseNodePropertyTypes(node->hasPt.get(), remapper);
        }

        if (node->lacksPt) {
            rebaseNodePropertyTypes(node->lacksPt.get(), remapper);
        }
    }

    std::unique_ptr<TrieNode> _root;
    std::map<EntityID, TrieNode*> _patchEntityToNode;
    std::vector<TrieNode*> _coreEntityToNode;
    EntityID _firstCoreEntityID {0};
    size_t _coreEntityCount {0};
};

} // namespace db
