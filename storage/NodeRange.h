#pragma once

#include "BioAssert.h"
#include "EntityID.h"

namespace db {

struct NodeRange {
    EntityID _first = 0;
    size_t _count = 0;

    bool hasEntity(EntityID entityID) const {
        return (entityID - _first) < _count;
    }

    class Iterator {
    public:
        Iterator() = default;

        Iterator(EntityID current, size_t left)
            : _current(current),
            _left(left)
        {
        }

        EntityID operator*() const {
            return _current;
        }

        Iterator& operator++() {
            bioassert(_left != 0);
            _current++;
            _left--;
            return *this;
        }

        Iterator& operator+=(size_t count) {
            _current += count;
            _left -= count;
            return *this;
        }

        bool operator==(const Iterator& other) const {
            return other._current == _current;
        }

        bool isValid() const {
            return _left != 0;
        }

    private:
        EntityID _current = 0;
        size_t _left = 0;
    };

    Iterator begin() const { return Iterator(_first, _count); }
    Iterator end() const { return Iterator(_first + _count, 0); }
};

}
