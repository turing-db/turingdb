#pragma once

#include <algorithm>
#include <memory>
#include <set>
#include <span>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ID.h"
#include "PropertyTypeSet.h"

namespace db {

class PropertyTypeTrie {
public:
    void build(const std::unordered_map<PropertyTypeID, std::span<const EntityID>>& typesToEntities) {
        if (_built) {
            throw std::logic_error("PropertyTypeTrie::build() called more than once");
        }

        // Step 1. Sort PropertyTypeIDs.
        //         Sorted order guarantees that two entities with the same set
        //         follow the exact same trie path and land on the same leaf node.
        std::vector<PropertyTypeID> sortedTypes;
        sortedTypes.reserve(typesToEntities.size());
        for (const auto& [ptId, entities] : typesToEntities) {
            sortedTypes.push_back(ptId);
        }
        std::sort(sortedTypes.begin(), sortedTypes.end());

        // Step 2. Collect all entity IDs into a sorted set (union of all spans).
        //         This will later be replaced by a pre-sorted vector from the caller.
        //         TODO: See if we can avoid this step and receive the sorted vector directly.
        std::set<EntityID> allEntities;
        for (const auto& [ptId, entities] : typesToEntities) {
            for (const EntityID e : entities) {
                allEntities.insert(e);
            }
        }

        // Step 3. Create the root node (empty PropertyTypeSet) and map every
        //         entity to it as the starting point.
        _root = std::make_unique<TrieNode>();
        _entityToNode.reserve(allEntities.size());
        for (const EntityID e : allEntities) {
            _entityToNode.emplace(e, _root.get());
        }

        // Step 4. For each PT in sorted order, merge-walk allEntities and the
        //         PT's span simultaneously, routing each entity to the appropriate
        //         child. Children are created lazily on first use.
        for (const PropertyTypeID ptId : sortedTypes) {
            const std::span<const EntityID> span = typesToEntities.at(ptId);
            auto spanIt = span.begin();

            for (const EntityID e : allEntities) {
                // Advance the span pointer to stay in sync with the set walk.
                while (spanIt != span.end() && *spanIt < e) {
                    ++spanIt;
                }

                TrieNode* cur = _entityToNode.at(e);
                const bool hasIt = (spanIt != span.end() && *spanIt == e);

                if (hasIt) {
                    if (!cur->hasPt) {
                        cur->hasPt = std::make_unique<TrieNode>();
                        cur->hasPt->propertyTypeSet = cur->propertyTypeSet;
                        cur->hasPt->propertyTypeSet.add(ptId); // sorted: ptId > all previous
                    }
                    _entityToNode[e] = cur->hasPt.get();
                    ++spanIt;
                } else {
                    if (!cur->lacksPt) {
                        cur->lacksPt = std::make_unique<TrieNode>();
                        cur->lacksPt->propertyTypeSet = cur->propertyTypeSet;
                    }
                    _entityToNode[e] = cur->lacksPt.get();
                }
            }
        }

        _built = true;
    }

    /** @brief Returns the PropertyTypeSet for the given entity. O(1).
     * The returned reference is stable for the lifetime of this object.
     *
     * @param entityId The entity ID.
     * @return The PropertyTypeSet.
     * */
    const PropertyTypeSet& getPropertyTypeSet(EntityID entityId) const {
        return _entityToNode.at(entityId)->propertyTypeSet;
    }

    /** @brief Returns the PropertyTypeSet for the given entity, or nullptr if
     * the entity is not tracked (i.e. has no properties). O(1).
     *
     * @param entityId The entity ID.
     * @return The PropertyTypeSet, or nullptr if the entity is not tracked.
     * */
    const PropertyTypeSet* tryGetPropertyTypeSet(EntityID entityId) const {
        const auto it = _entityToNode.find(entityId);
        if (it == _entityToNode.end()) {
            return nullptr;
        }

        return &it->second->propertyTypeSet;
    }

    /** @brief Returns true if the given entity has the given property type. O(log P).
     * 
     * @param entityId The entity ID.
     * @param ptId The property type ID.
     * @return True if the entity has the property type, false otherwise.
     * */
    bool hasPropertyType(EntityID entityId, PropertyTypeID ptId) const {
        const PropertyTypeSet& set = _entityToNode.at(entityId)->propertyTypeSet;
        return std::binary_search(set.begin(), set.end(), ptId);
    }

    /** @brief Returns the number of unique PropertyTypeSets interned.
     *
     * @return The number of unique PropertyTypeSets interned.
     * */
    size_t typeSetCount() const {
        std::unordered_set<const TrieNode*> unique;
        for (const auto& [e, node] : _entityToNode) {
            unique.insert(node);
        }

        return unique.size();
    }

    /** @brief Returns the number of entities registered.
     * 
     * @return The number of entities registered.
     * */
    size_t entityCount() const {
        return _entityToNode.size();
    }

private:
    struct TrieNode {
        PropertyTypeSet propertyTypeSet;
        std::unique_ptr<TrieNode> lacksPt;
        std::unique_ptr<TrieNode> hasPt;
    };

    std::unique_ptr<TrieNode> _root;
    std::unordered_map<EntityID, TrieNode*> _entityToNode;
    bool _built = false;
};

} // namespace db
