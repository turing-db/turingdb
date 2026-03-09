#include "PropertyTombstoneSet.h"

#include "versioning/EntityIDRebaser.h"

using namespace db;

void PropertyTombstoneSet::add(PropertyTypeID ptID, EntityID entityID) {
    _entries[ptID].insert(entityID);
}

bool PropertyTombstoneSet::contains(PropertyTypeID ptID, EntityID entityID) const {
    auto it = _entries.find(ptID);
    if (it == _entries.end()) {
        return false;
    }
    return it->second.contains(entityID);
}

bool PropertyTombstoneSet::empty() const {
    return _entries.empty();
}

void PropertyTombstoneSet::markSeen(EntityID entityID,
                                     std::unordered_set<PropertyTypeID>& seen) const {
    for (const auto& [ptID, ids] : _entries) {
        if (ids.contains(entityID)) {
            seen.insert(ptID);
        }
    }
}

void PropertyTombstoneSet::rebaseIDs(EntityIDRebaser& rebaser, bool isNode) {
    for (auto& [ptID, ids] : _entries) {
        std::unordered_set<EntityID> rebased(ids.size());
        for (const EntityID id : ids) {
            const EntityID newID = isNode
                ? rebaser.rebaseNodeID(NodeID {id.getValue()}).getValue()
                : rebaser.rebaseEdgeID(EdgeID {id.getValue()}).getValue();
            rebased.insert(newID);
        }
        ids.swap(rebased);
    }
}
