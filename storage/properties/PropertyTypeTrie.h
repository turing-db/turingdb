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

namespace db {

// A sorted list of all PropertyTypeIDs an entity has.
// Sorted order is guaranteed by the build phase and enables binary search.
using PropertyTypeSet = std::vector<PropertyTypeID>;

// ---------------------------------------------------------------------------
// PropertyTypeTrie
// ---------------------------------------------------------------------------
// Maps every EntityID to its exact PropertyTypeSet, deduplicating identical
// sets so that each unique set is stored only once.
//
// RESULT LAYOUT
//
//   _root                           — root TrieNode (empty PropertyTypeSet)
//   _entityToNode[entityId]         — TrieNode* holding the entity's PropertyTypeSet
//   node->propertyTypeSet           — the canonical sorted PropertyTypeSet
//
//   A lookup is one hash map access + one pointer dereference.
//
// BUILD STRATEGY — persistent merge-walk trie
//
//   We maintain a single persistent trie where each node stores its
//   PropertyTypeSet directly. All entities start at the root (empty set).
//   Property types are processed one by one in sorted order.
//
//   For each PT we merge-walk the sorted global entity set and the sorted
//   span of EntityIDs that have this PT simultaneously:
//     - entities that HAVE this PT  →  follow hasPt  child (set gains ptId)
//     - entities that LACK this PT  →  follow lacksPt child (set unchanged)
//
//   Children are created lazily: a hasPt child copies the parent's set and
//   appends ptId (appending preserves sorted order since PTs are processed
//   in ascending order); a lacksPt child copies the parent's set as-is.
//
//   Entities that follow the same sequence of has/lacks decisions share the
//   same TrieNode — identical PropertyTypeSets are naturally deduplicated.
//   The trie remains alive after build for O(1) lookups.
//
// MEMORY
//   _entityToNode :  entityCount  × sizeof(pointer + hash overhead)
//   trie nodes    :  one per unique PropertyTypeSet (negligible)
// ---------------------------------------------------------------------------
class PropertyTypeTrie {
public:
    // -----------------------------------------------------------------------
    // Build
    // -----------------------------------------------------------------------

    // Constructs the entityId → PropertyTypeSet mapping.
    //
    // typesToEntities  — PropertyTypeID → sorted span of EntityIDs that have that type.
    //                 An EntityID may appear in multiple spans.
    //                 EntityIDs absent from all spans are not tracked.
    void build(const std::unordered_map<PropertyTypeID, std::span<const EntityID>>& typesToEntities) {
        if (_built) {
            throw std::logic_error("PropertyTypeTrie::build() called more than once");
        }

        // -----------------------------------------------------------------
        // 1. Sort PropertyTypeIDs.
        //    Sorted order guarantees that two entities with the same set
        //    follow the exact same trie path and land on the same leaf node.
        // -----------------------------------------------------------------
        std::vector<PropertyTypeID> sortedTypes;
        sortedTypes.reserve(typesToEntities.size());
        for (const auto& [ptId, entities] : typesToEntities) {
            sortedTypes.push_back(ptId);
        }
        std::sort(sortedTypes.begin(), sortedTypes.end());

        // -----------------------------------------------------------------
        // 2. Collect all entity IDs into a sorted set (union of all spans).
        //    This will later be replaced by a pre-sorted vector from the caller.
        // -----------------------------------------------------------------
        std::set<EntityID> allEntities;
        for (const auto& [ptId, entities] : typesToEntities) {
            for (const EntityID e : entities) {
                allEntities.insert(e);
            }
        }

        // -----------------------------------------------------------------
        // 3. Create the root node (empty PropertyTypeSet) and map every
        //    entity to it as the starting point.
        // -----------------------------------------------------------------
        _root = std::make_unique<TrieNode>();
        _entityToNode.reserve(allEntities.size());
        for (const EntityID e : allEntities) {
            _entityToNode.emplace(e, _root.get());
        }

        // -----------------------------------------------------------------
        // 4. For each PT in sorted order, merge-walk allEntities and the
        //    PT's span simultaneously, routing each entity to the appropriate
        //    child. Children are created lazily on first use.
        // -----------------------------------------------------------------
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
                        cur->hasPt->propertyTypeSet.push_back(ptId); // sorted: ptId > all previous
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

    // -----------------------------------------------------------------------
    // Getters
    // -----------------------------------------------------------------------

    // Returns the sorted PropertyTypeSet for the given entity. O(1).
    // The returned reference is stable for the lifetime of this object.
    const PropertyTypeSet& getPropertyTypeSet(EntityID entityId) const {
        assertBuilt();
        assertValidEntity(entityId);
        return _entityToNode.at(entityId)->propertyTypeSet;
    }

    // Returns the sorted PropertyTypeSet for the given entity, or nullptr if
    // the entity is not tracked (i.e. has no properties). O(1).
    const PropertyTypeSet* tryGetPropertyTypeSet(EntityID entityId) const {
        assertBuilt();
        const auto it = _entityToNode.find(entityId);
        if (it == _entityToNode.end()) return nullptr;
        return &it->second->propertyTypeSet;
    }

    // Returns true if the given entity has the given property type. O(log P).
    bool hasPropertyType(EntityID entityId, PropertyTypeID ptId) const {
        assertBuilt();
        assertValidEntity(entityId);
        const PropertyTypeSet& set = _entityToNode.at(entityId)->propertyTypeSet;
        return std::binary_search(set.begin(), set.end(), ptId);
    }

    // Returns the number of unique PropertyTypeSets interned.
    size_t setCount() const {
        assertBuilt();
        std::unordered_set<const TrieNode*> unique;
        for (const auto& [e, node] : _entityToNode) {
            unique.insert(node);
        }
        return unique.size();
    }

    // Returns the number of entities registered.
    size_t entityCount() const {
        assertBuilt();
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

    void assertBuilt() const {
        if (!_built) {
            throw std::logic_error("PropertyTypeTrie has not been built yet");
        }
    }

    void assertValidEntity(EntityID entityId) const {
        if (_entityToNode.find(entityId) == _entityToNode.end()) {
            throw std::out_of_range("EntityID not found in PropertyTypeTrie");
        }
    }
};

} // namespace db
