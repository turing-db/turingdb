#include "PathReachTable.h"

#include "BioAssert.h"

using namespace db;

PathReachTable::PathReachTable() {
    allocate(initialCapacity);
}

PathReachTable::~PathReachTable() {
}

PathReachTable::Slot& PathReachTable::reach(NodeID node) {
    if ((_count + 1) * 2 > _slots.size()) {
        grow();
    }

    const uint64_t key = node.getValue();
    size_t slot = hashOf(key) & _mask;

    while (_slots[slot]._node != emptyKey) {
        if (_slots[slot]._node == key) {
            return _slots[slot];
        }

        slot = (slot + 1) & _mask;
    }

    _slots[slot]._node = key;
    _occupied.push_back(slot);
    _count++;

    return _slots[slot];
}

PathReachTable::Slot& PathReachTable::get(NodeID node) {
    const size_t slot = find(node.getValue());
    bioassert(slot != _slots.size(), "The node was never reached");

    return _slots[slot];
}

void PathReachTable::clear() {
    for (const size_t slot : _occupied) {
        _slots[slot] = Slot {};
    }

    _occupied.clear();
    _count = 0;
}

size_t PathReachTable::find(uint64_t key) const {
    size_t slot = hashOf(key) & _mask;

    while (_slots[slot]._node != emptyKey) {
        if (_slots[slot]._node == key) {
            return slot;
        }

        slot = (slot + 1) & _mask;
    }

    return _slots.size();
}

void PathReachTable::allocate(size_t capacity) {
    _slots.assign(capacity, Slot {});
    _occupied.clear();
    _mask = capacity - 1;
    _count = 0;
}

void PathReachTable::grow() {
    std::vector<Slot> slots;
    slots.swap(_slots);

    std::vector<size_t> occupied;
    occupied.swap(_occupied);

    allocate(slots.size() * 2);

    for (const size_t oldSlot : occupied) {
        const Slot& reached = slots[oldSlot];
        size_t slot = hashOf(reached._node) & _mask;
        while (_slots[slot]._node != emptyKey) {
            slot = (slot + 1) & _mask;
        }

        _slots[slot] = reached;
        _occupied.push_back(slot);
    }

    _count = occupied.size();
}
