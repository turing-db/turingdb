#include "PathCycleTable.h"

#include "BioAssert.h"

using namespace db;

PathCycleTable::PathCycleTable()
    : _slots(initialCapacity, emptySlot),
    _mask(initialCapacity - 1)
{
}

PathCycleTable::~PathCycleTable() {
}

void PathCycleTable::reset(size_t wordCount) {
    for (const size_t slot : _occupied) {
        _slots[slot] = emptySlot;
    }

    _occupied.clear();
    _nodes.clear();
    _words.clear();
    _wordCount = wordCount;
}

uint64_t* PathCycleTable::reach(NodeID node) {
    if ((_nodes.size() + 1) * 2 > _slots.size()) {
        grow();
    }

    const uint64_t key = node.getValue();
    const size_t slot = slotOf(key);
    if (_slots[slot] != emptySlot) {
        return &_words[_slots[slot] * _wordCount];
    }

    bioassert(_nodes.size() < emptySlot, "Too many nodes reached in one batch");

    const size_t index = _nodes.size();
    _slots[slot] = static_cast<uint32_t>(index);
    _occupied.push_back(slot);
    _nodes.push_back(key);
    _words.resize(_words.size() + _wordCount, 0);

    return &_words[index * _wordCount];
}

uint64_t* PathCycleTable::find(NodeID node) {
    const size_t slot = slotOf(node.getValue());
    if (_slots[slot] == emptySlot) {
        return nullptr;
    }

    return &_words[_slots[slot] * _wordCount];
}

size_t PathCycleTable::slotOf(uint64_t key) const {
    size_t slot = hashOf(key) & _mask;

    while (_slots[slot] != emptySlot && _nodes[_slots[slot]] != key) {
        slot = (slot + 1) & _mask;
    }

    return slot;
}

void PathCycleTable::grow() {
    const size_t capacity = _slots.size() * 2;
    _slots.assign(capacity, emptySlot);
    _occupied.clear();
    _mask = capacity - 1;

    for (size_t index = 0; index < _nodes.size(); index++) {
        const size_t slot = slotOf(_nodes[index]);
        _slots[slot] = static_cast<uint32_t>(index);
        _occupied.push_back(slot);
    }
}
