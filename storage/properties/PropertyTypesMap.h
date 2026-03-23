#pragma once

#include <algorithm>
#include <ranges>
#include <stdexcept>
#include <numeric>
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
// RESULT LAYOUT (the only two structures that survive after build)
//
//   _entityToSetId[entityId]  →  uint32_t setId
//   _sets[setId]          →  PropertyTypeSet (sorted, unique)
//
//   A lookup is two array accesses: _sets[_entityToSetId[entityId]]
//
// BUILD STRATEGY — partition refinement via a temporary binary trie
//
//   A naive inversion (entityId → collect all ptIds → store vector) allocates
//   one vector per node and never deduplicates. Instead we use a trie as a
//   build-time tool:
//
//   We maintain a list of "groups". Each group is a set of EntityIDs that are
//   currently indistinguishable — they have seen exactly the same property
//   types so far. We process property types one by one in sorted order. For
//   each one we split every group in two:
//     - entities that HAVE this PT  →  descend into hasPt  child
//     - entities that LACK this PT  →  descend into lacksPt child
//
//   After all P property types are processed, entities in the same group have
//   identical PropertyTypeSets and map to the same trie leaf → same setId.
//   Two nodes sharing a leaf share one PropertyTypeSet entry in _sets.
//
//   The trie and all Group objects are destroyed at the end of build().
//   Only _entityToSetId and _sets remain.
//
// MEMORY DURING BUILD
//   At every level the total number of EntityIDs across all groups is exactly
//   entityCount — we only repartition, never duplicate. Peak cost is O(N + P).
//
// MEMORY AFTER BUILD
//   _entityToSetId :  entityCount  × sizeof(uint32_t)       (4 bytes/entity)
//   _sets          :  uniqueSets × sizeof(PropertyTypeSet)  (negligible)
// ---------------------------------------------------------------------------
class PropertyTypeTrie {
public:
    // -----------------------------------------------------------------------
    // Build
    // -----------------------------------------------------------------------

    // Constructs the entityId → PropertyTypeSet mapping.
    //
    // typesToEntities  — PropertyTypeID → list of EntityIDs that have that type.
    //                 A EntityID may appear in multiple lists.
    //                 EntityIDs absent from all lists are mapped to the empty set.
    // entityCount     — total number of nodes. Every EntityID in [0, entityCount)
    //                 is valid after build.
    void build(const std::unordered_map<PropertyTypeID, std::vector<EntityID>>& typesToEntities,
               size_t entityCount) {
        if (_built) {
            throw std::logic_error("PropertyTypeTrie::build() called more than once");
        }

        // -----------------------------------------------------------------
        // 1. Collect and sort PropertyTypeIDs.
        //    Sorted order guarantees that two entities with the same set always
        //    walk the exact same trie path and land on the same leaf.
        // -----------------------------------------------------------------
        std::vector<PropertyTypeID> sortedTypes(typesToEntities.size());
        for (const auto& [i, ptId] : typesToEntities
                                         | std::views::keys
                                         | std::views::enumerate) {
            sortedTypes[i] = ptId;
        }
        std::sort(sortedTypes.begin(), sortedTypes.end());

        // -----------------------------------------------------------------
        // 2. Seed the partition with one group holding every EntityID.
        //    All entities start at the trie root — indistinguishable so far.
        // -----------------------------------------------------------------
        TrieNode root;

        std::vector<Group> groups(1);
        groups[0].node = &root;
        groups[0].entityIds.resize(entityCount);
        std::iota(groups[0].entityIds.begin(), groups[0].entityIds.end(), EntityID {0});

        // -----------------------------------------------------------------
        // 3. Partition refinement — split groups level by level.
        //
        //    For each PT, every group is split into (at most) two children.
        //    Groups are temporary: their entityIds lists are just being
        //    reshuffled. Nothing is written into the trie nodes themselves —
        //    trie nodes here serve only as stable addresses that let us
        //    recognise when two groups converge on the same leaf.
        // -----------------------------------------------------------------
        for (const PropertyTypeID ptId : sortedTypes) {
            const std::vector<EntityID>& members = typesToEntities.at(ptId);

            // Hash set for O(1) membership tests during the split.
            const std::unordered_set<EntityID> memberSet(members.begin(), members.end());

            // Each existing group produces at most 2 children.
            std::vector<Group> nextGroups;
            nextGroups.reserve(groups.size() * 2);

            for (Group& g : groups) {
                Group hasIt;   // entities in g that DO     have ptId
                Group lacksIt; // entities in g that DO NOT have ptId

                for (const EntityID nid : g.entityIds) {
                    if (memberSet.contains(nid)) {
                        hasIt.entityIds.push_back(nid);
                    } else {
                        lacksIt.entityIds.push_back(nid);
                    }
                }

                // Create child trie nodes lazily — only if the sub-group is
                // non-empty. This prunes all branches no real data reaches.
                auto emitChild = [&](Group& child, std::unique_ptr<TrieNode>& slot,
                                     bool hasPtBranch) {
                    if (child.entityIds.empty()) {
                        return;
                    }
                    if (!slot) {
                        slot = std::make_unique<TrieNode>();
                        slot->parent = g.node;
                        slot->splitPt = ptId;
                        slot->hasPtBranch = hasPtBranch;
                    }
                    child.node = slot.get();
                    nextGroups.push_back(std::move(child));
                };

                emitChild(lacksIt, g.node->lacksPt, false);
                emitChild(hasIt, g.node->hasPt, true);
            }

            groups = std::move(nextGroups);
        }

        // -----------------------------------------------------------------
        // 4. Assign set IDs and populate the output structures.
        //
        //    Every surviving group is a unique leaf. We materialise its
        //    PropertyTypeSet by walking the parent chain (O(P) per unique
        //    set, called once per leaf — not once per entity), assign it a
        //    setId, and stamp every EntityID in the group with that setId.
        //
        //    After this loop the trie goes out of scope and is destroyed.
        //    Only _entityToSetId and _sets remain.
        // -----------------------------------------------------------------
        _entityToSetId.resize(entityCount, INVALID_SET_ID);
        _sets.reserve(groups.size());

        for (const Group& g : groups) {
            if (g.node->setId == INVALID_SET_ID) {
                g.node->setId = static_cast<uint32_t>(_sets.size());
                _sets.push_back(buildSetFromPath(g.node));
            }
            for (const EntityID nid : g.entityIds) {
                _entityToSetId[nid.getValue()] = g.node->setId;
            }
        }

        _built = true;
        // root and all TrieNodes are destroyed here — trie is gone.
    }

    // -----------------------------------------------------------------------
    // Getters
    // -----------------------------------------------------------------------

    // Returns the sorted PropertyTypeSet for the given entity. O(1).
    // The returned reference is stable for the lifetime of this object.
    const PropertyTypeSet& getPropertyTypeSet(EntityID entityId) const {
        assertBuilt();
        assertValidEntity(entityId);
        return _sets[_entityToSetId[entityId.getValue()]];
    }

    // Returns true if the given entity has the given property type. O(log P).
    bool hasPropertyType(EntityID entityId, PropertyTypeID ptId) const {
        assertBuilt();
        assertValidEntity(entityId);
        const PropertyTypeSet& set = _sets[_entityToSetId[entityId.getValue()]];
        return std::binary_search(set.begin(), set.end(), ptId);
    }

    // Returns the number of unique PropertyTypeSets interned.
    size_t setCount() const {
        assertBuilt();
        return _sets.size();
    }

    // Returns the number of entities registered.
    size_t entityCount() const {
        assertBuilt();
        return _entityToSetId.size();
    }

private:
    // -----------------------------------------------------------------------
    // Internal types — used only during build(), destroyed afterwards
    // -----------------------------------------------------------------------

    // A node in the temporary build-time trie.
    // Carries no entity IDs — only the tree structure and a set ID once finalised.
    struct TrieNode {
        std::unique_ptr<TrieNode> lacksPt; // branch: lacks current PT
        std::unique_ptr<TrieNode> hasPt;   // branch: has   current PT
        TrieNode* parent = nullptr;        // null on root
        PropertyTypeID splitPt = 0;        // PT that produced this node
        bool hasPtBranch = false;          // true if arrived via hasPt
        uint32_t setId = INVALID_SET_ID;   // assigned at leaf finalisation
    };

    // Temporary group: EntityIDs sharing the same trie path so far.
    // Exists only during build() — not stored as a member.
    struct Group {
        TrieNode* node = nullptr;
        std::vector<EntityID> entityIds;
    };

    // -----------------------------------------------------------------------
    // Persistent state — the only data that survives build()
    // -----------------------------------------------------------------------

    std::vector<uint32_t> _entityToSetId; // entityId  → index into _sets
    std::vector<PropertyTypeSet> _sets;   // setId   → canonical sorted PropertyTypeSet
    bool _built = false;

    static constexpr uint32_t INVALID_SET_ID = UINT32_MAX;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    // Reconstructs the PropertyTypeSet for a leaf by walking up to the root
    // and collecting the splitPt of every hasPt edge along the path.
    // Called once per unique leaf during build — not performance-critical.
    static PropertyTypeSet buildSetFromPath(const TrieNode* leaf) {
        PropertyTypeSet set;
        for (const TrieNode* cur = leaf; cur->parent != nullptr; cur = cur->parent) {
            if (cur->hasPtBranch) {
                set.push_back(cur->splitPt);
            }
        }
        // Walk goes leaf→root so IDs are in reverse order; sort to restore
        // canonical ascending order (which enables binary search on lookup).
        std::sort(set.begin(), set.end());
        return set;
    }

    void assertBuilt() const {
        if (!_built) {
            throw std::logic_error("PropertyTypeTrie has not been built yet");
        }
    }

    void assertValidEntity(EntityID entityId) const {
        if (entityId.getValue() >= _entityToSetId.size()) {
            throw std::out_of_range("EntityID out of range");
        }
    }
};

} // namespace db
