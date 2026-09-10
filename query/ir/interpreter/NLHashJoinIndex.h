#pragma once

#include <stddef.h>
#include <stdint.h>
#include <limits>
#include <vector>

namespace db {

// The index of a hash join's build side: the rows carrying one key form a group, and one
// bucket chains the groups of every key falling in it. It holds hashes and row numbers
// only - a key stays in the build buffer, where the caller compares it - so walking a
// bucket confirms each group's key once and the rows within a group need no comparison.
class NLHashJoinIndex {
public:
    static constexpr size_t noRow = std::numeric_limits<size_t>::max();

    NLHashJoinIndex();
    ~NLHashJoinIndex();

    void clear();

    // Extend the row set by count rows numbered from getRowCount(), none indexed yet.
    void addRows(size_t count);

    void openGroup(size_t row, uint64_t hash);

    // Add a row to the group opened at head, whose key it carries. The group keeps the
    // order its rows were added in, which is the order the probe emits its matches in.
    void addToGroup(size_t head, size_t row);

    size_t getRowCount() const { return _hashes.size(); }

    size_t getFirstGroup(uint64_t hash) const;
    size_t getNextGroup(size_t head) const { return _nextGroups[head]; }

    size_t getNextInGroup(size_t row) const { return _nextInGroup[row]; }

    uint64_t getHash(size_t row) const { return _hashes[row]; }

private:
    std::vector<uint64_t> _hashes;
    std::vector<size_t> _nextGroups;
    std::vector<size_t> _nextInGroup;
    std::vector<size_t> _groupTails;
    std::vector<size_t> _buckets;
    size_t _groupCount {0};

    void growBuckets();
    void chainGroup(size_t head, uint64_t hash);
};

}
