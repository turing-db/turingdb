#pragma once

#include <limits>
#include <map>
#include <span>
#include <unordered_map>
#include <vector>

#include "ID.h"
#include "PropertyTypeSet.h"

namespace db {

class PropertyTypeTrie {
public:
    using TypeToIDsMap = std::unordered_map<PropertyTypeID, std::span<const EntityID>>;
    static constexpr uint16_t INVALID_NODE = std::numeric_limits<uint16_t>::max();

    PropertyTypeTrie();
    ~PropertyTypeTrie();

    void build(const TypeToIDsMap& typesToEntities,
               EntityID firstCoreEntityID,
               size_t coreEntityCount);

    /** @brief Returns the PropertyTypeSet for the given entity. O(1).
     * The returned reference is stable for the lifetime of this object.
     *
     * @param entityId The entity ID.
     * @return The PropertyTypeSet.
     * */
    const PropertyTypeSet& getPropertyTypeSet(EntityID entityId) const;

    /** @brief Returns the PropertyTypeSet for the given entity, or nullptr if
     * the entity is not tracked (i.e. has no properties). O(1).
     *
     * @param entityId The entity ID.
     * @return The PropertyTypeSet, or nullptr if the entity is not tracked.
     * */
    const PropertyTypeSet* tryGetPropertyTypeSet(EntityID entityId) const;

    /** @brief Returns true if the given entity has the given property type. O(log P).
     *
     * @param entityId The entity ID.
     * @param ptId The property type ID.
     * @return True if the entity has the property type, false otherwise.
     * */
    bool hasPropertyType(EntityID entityId, PropertyTypeID ptId) const;

    /** @brief Returns the number of entities registered (i.e. with at least one property type).
     *
     * @return The number of entities registered.
     * */
    size_t entityCount() const;

    /** @brief Remaps all PropertyTypeIDs stored in every trie node in-place.
     *  The trie structure (entity-to-node mapping) is unchanged; only the PT ID
     *  values inside each node's PropertyTypeSet are updated.
     *
     * @param remapper A callable (PropertyTypeID) -> PropertyTypeID applied to every stored ID.
     * */
    template <typename Remapper>
    void rebasePropertyTypes(const Remapper& remapper) {
        for (TrieNode& node : _nodes) {
            for (PropertyTypeID& id : node._propertyTypeSet.get()) {
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
        PropertyTypeSet _propertyTypeSet;
        uint16_t _lacksPt {INVALID_NODE};
        uint16_t _hasPt {INVALID_NODE};
    };

    uint16_t allocNode();

    std::vector<TrieNode> _nodes;
    std::map<EntityID, uint16_t> _patchEntityToNode;
    std::vector<uint16_t> _coreEntityToNode;
    EntityID _firstCoreEntityID {0};
    size_t _coreEntityCount {0};
};

} // namespace db
