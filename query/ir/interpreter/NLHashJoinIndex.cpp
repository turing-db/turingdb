#include "NLHashJoinIndex.h"

#include <algorithm>

using namespace db;

namespace {

constexpr size_t minimumBucketCount = 16;

}

NLHashJoinIndex::NLHashJoinIndex() {
}

NLHashJoinIndex::~NLHashJoinIndex() {
}

void NLHashJoinIndex::clear() {
    _hashes.clear();
    _nextGroups.clear();
    _nextInGroup.clear();
    _groupTails.clear();
    _buckets.clear();
    _groupCount = 0;
}

void NLHashJoinIndex::addRows(size_t count) {
    const size_t rowCount = _hashes.size() + count;

    _hashes.resize(rowCount, 0);
    _nextGroups.resize(rowCount, noRow);
    _nextInGroup.resize(rowCount, noRow);
    _groupTails.resize(rowCount, noRow);
}

void NLHashJoinIndex::openGroup(size_t row, uint64_t hash) {
    _hashes[row] = hash;
    _nextInGroup[row] = noRow;
    _groupTails[row] = row;
    _groupCount++;

    if (_groupCount > _buckets.size()) {
        growBuckets();
    }

    chainGroup(row, hash);
}

void NLHashJoinIndex::addToGroup(size_t head, size_t row) {
    _hashes[row] = _hashes[head];
    _nextInGroup[row] = noRow;

    _nextInGroup[_groupTails[head]] = row;
    _groupTails[head] = row;
}

size_t NLHashJoinIndex::getFirstGroup(uint64_t hash) const {
    if (_buckets.empty()) {
        return noRow;
    }

    return _buckets[hash & (_buckets.size() - 1)];
}

void NLHashJoinIndex::growBuckets() {
    std::vector<size_t> oldBuckets;
    oldBuckets.swap(_buckets);

    const size_t bucketCount = std::max(minimumBucketCount, oldBuckets.size() * 2);
    _buckets.assign(bucketCount, noRow);

    for (const size_t bucket : oldBuckets) {
        size_t head = bucket;

        while (head != noRow) {
            const size_t next = _nextGroups[head];
            chainGroup(head, _hashes[head]);
            head = next;
        }
    }
}

void NLHashJoinIndex::chainGroup(size_t head, uint64_t hash) {
    const size_t bucket = hash & (_buckets.size() - 1);

    _nextGroups[head] = _buckets[bucket];
    _buckets[bucket] = head;
}
