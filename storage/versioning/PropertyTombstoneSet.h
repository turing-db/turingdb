#pragma once

#include <unordered_map>
#include <unordered_set>

#include "ID.h"

namespace db {

class EntityIDRebaser;

class PropertyTombstoneSet {
public:
    void add(PropertyTypeID ptID, EntityID entityID);
    bool contains(PropertyTypeID ptID, EntityID entityID) const;
    bool empty() const;

    void markSeen(EntityID entityID, std::unordered_set<PropertyTypeID>& seen) const;

    void rebaseIDs(EntityIDRebaser& rebaser, bool isNode);

private:
    std::unordered_map<PropertyTypeID, std::unordered_set<EntityID>> _entries;
};

}
