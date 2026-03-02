#pragma once

#include "EntityType.h"
#include "ID.h"

namespace db {

class EntityList {
public:
    struct Entry {
        EntityType _type;
        EntityID _id;

        bool operator==(const Entry& other) const {
            return _type == other._type && _id == other._id;
        }
    };

    EntityList();
    ~EntityList();

    EntityList(const EntityList&);
    EntityList(EntityList&&) noexcept;
    EntityList& operator=(const EntityList&);
    EntityList& operator=(EntityList&&) noexcept;

    void add(EntityType type, EntityID id) {
        _entries.emplace_back(type, id);
    }

    bool empty() const { return _entries.empty(); }
    size_t size() const { return _entries.size(); }

    std::span<const Entry> entries() const { return _entries; }
    std::vector<Entry>::const_iterator begin() const { return _entries.begin(); }
    std::vector<Entry>::const_iterator end() const { return _entries.end(); }

private:
    std::vector<Entry> _entries;
};

}
