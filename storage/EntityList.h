#pragma once

#include "EntityType.h"
#include "ID.h"

namespace db {

class EntityList {
public:
    struct Entry {
        EntityType _type {};
        EntityID _id;

        bool operator==(const Entry& other) const {
            return _type == other._type && _id == other._id;
        }
    };

    using Container = std::vector<Entry>;

    EntityList();
    ~EntityList();

    EntityList(const EntityList&);
    EntityList(EntityList&&) noexcept;
    EntityList& operator=(const EntityList&);
    EntityList& operator=(EntityList&&) noexcept;

    void add(EntityType type, EntityID id) {
        _entries.emplace_back(type, id);
    }

    void add() {
        _entries.emplace_back();
    }

    Entry& back() {
        return _entries.back();
    }

    void assign(Container::const_iterator first, Container::const_iterator last) {
        _entries.assign(first, last);
    }

    void clear() {
        _entries.clear();
    }

    void resize(size_t size) {
        _entries.resize(size);
    }

    void reserve(size_t size) {
        _entries.reserve(size);
    }

    EntityList::Entry& operator[](size_t idx) {
        return _entries[idx];
    }

    bool empty() const { return _entries.empty(); }
    size_t size() const { return _entries.size(); }
    size_t capacity() const { return _entries.capacity(); }
    const Container& getEntries() const { return _entries; }

    Container::const_iterator begin() const { return _entries.begin(); }
    Container::const_iterator end() const { return _entries.end(); }

private:
    Container _entries;
};

}
