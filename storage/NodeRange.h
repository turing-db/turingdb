#pragma once

#include "ID.h"

namespace db {

struct NodeRange {
    NodeID _first = 0;
    size_t _count = 0;

    bool hasEntity(NodeID entityID) const {
        return (entityID - _first) < _count;
    }

    class Iterator {
    public:
        Iterator() = default;

        Iterator(NodeID current, size_t left)
            : _current(current),
            _left(left)
        {
        }

        NodeID operator*() const {
            return _current;
        }

        Iterator& operator++() {
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
        NodeID _current = 0;
        size_t _left = 0;
    };

    Iterator begin() const { return Iterator(_first, _count); }
    Iterator end() const { return Iterator(_first + _count, 0); }
};

}
