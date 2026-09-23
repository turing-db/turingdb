#include <gtest/gtest.h>

#include <vector>

#include "list/ListBuffer.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "list/PathTrie.h"

#include "ID.h"

using namespace db;

namespace {

class PathTrieTest : public testing::Test {
protected:
    PathTrieTest()
        : _arena(_trie.acquireArena())
    {
    }

    template <typename IDType>
    void readIDs(const ListView& list, std::vector<uint64_t>& ids) {
        ids.clear();
        for (const ListElementView& element : list) {
            ids.push_back(element.getAs<IDType>().getValue());
        }
    }

    PathTrie _trie;
    size_t _arena {0};
    ListBuffer<> _buffer;
};

}

TEST_F(PathTrieTest, rootIsTheEmptyPath) {
    EXPECT_EQ(_trie.size(), 1u);
    EXPECT_EQ(_trie.getDepth(PathTrie::ROOT), 0u);
    EXPECT_EQ(PathTrie::arenaOf(PathTrie::ROOT), 0u);
    EXPECT_NE(_arena, 0u);

    std::vector<uint64_t> ids;

    readIDs<EdgeID>(_trie.expandEdges(PathTrie::ROOT, _buffer, false), ids);
    EXPECT_TRUE(ids.empty());

    readIDs<NodeID>(_trie.expandEnds(PathTrie::ROOT, _buffer, false), ids);
    EXPECT_TRUE(ids.empty());

    readIDs<NodeID>(_trie.expandSources(PathTrie::ROOT, NodeID(7), _buffer, false), ids);
    EXPECT_TRUE(ids.empty());
}

TEST_F(PathTrieTest, expandsAChainRootFirst) {
    const PathRef first = _trie.append(_arena, PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    const PathRef second = _trie.append(_arena, first, EdgeID(11), NodeID(2), 2);
    const PathRef third = _trie.append(_arena, second, EdgeID(12), NodeID(3), 3);

    EXPECT_EQ(_trie.getDepth(third), 3u);
    EXPECT_EQ(_trie.get(third)._parent, second);
    EXPECT_EQ(_trie.get(second)._parent, first);
    EXPECT_EQ(_trie.get(first)._parent, PathTrie::ROOT);

    std::vector<uint64_t> ids;

    readIDs<EdgeID>(_trie.expandEdges(third, _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10, 11, 12}));

    readIDs<NodeID>(_trie.expandEnds(third, _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {1, 2, 3}));

    readIDs<NodeID>(_trie.expandSources(third, NodeID(0), _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {0, 1, 2}));

    readIDs<EdgeID>(_trie.expandEdges(first, _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10}));

    readIDs<NodeID>(_trie.expandSources(first, NodeID(0), _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {0}));
}

TEST_F(PathTrieTest, sharedPrefixIsStoredOnce) {
    const PathRef prefix = _trie.append(_arena, PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    const PathRef left = _trie.append(_arena, prefix, EdgeID(11), NodeID(2), 2);
    const PathRef right = _trie.append(_arena, prefix, EdgeID(12), NodeID(3), 2);

    // The root, the prefix and the two leaves: the prefix is not repeated per path
    EXPECT_EQ(_trie.size(), 4u);

    std::vector<uint64_t> ids;

    readIDs<EdgeID>(_trie.expandEdges(left, _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10, 11}));

    readIDs<EdgeID>(_trie.expandEdges(right, _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10, 12}));
}

TEST_F(PathTrieTest, clearKeepsOnlyTheRoot) {
    _trie.append(_arena, PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    _trie.clear();

    EXPECT_EQ(_trie.size(), 1u);
    EXPECT_EQ(_trie.getDepth(PathTrie::ROOT), 0u);

    const size_t arena = _trie.acquireArena();
    const PathRef first = _trie.append(arena, PathTrie::ROOT, EdgeID(20), NodeID(5), 1);
    EXPECT_EQ(PathTrie::arenaOf(first), arena);
    EXPECT_EQ(PathTrie::indexOf(first), 0u);
    EXPECT_EQ(_trie.size(), 2u);
}

TEST_F(PathTrieTest, arenasHoldTheirOwnEntries) {
    const size_t other = _trie.acquireArena();
    EXPECT_NE(other, _arena);

    const PathRef mine = _trie.append(_arena, PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    const PathRef theirs = _trie.append(other, PathTrie::ROOT, EdgeID(20), NodeID(2), 1);
    EXPECT_NE(mine, theirs);
    EXPECT_EQ(PathTrie::arenaOf(mine), _arena);
    EXPECT_EQ(PathTrie::indexOf(mine), 0u);
    EXPECT_EQ(PathTrie::arenaOf(theirs), other);
    EXPECT_EQ(PathTrie::indexOf(theirs), 0u);
    EXPECT_EQ(_trie.size(), 3u);

    std::vector<uint64_t> ids;
    readIDs<EdgeID>(_trie.expandEdges(theirs, _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {20}));

    _trie.releaseArena(other);
    EXPECT_EQ(_trie.size(), 2u);
    EXPECT_EQ(_trie.acquireArena(), other);
    EXPECT_EQ(_trie.getArenaSize(other), 0u);
}

TEST_F(PathTrieTest, truncateDropsTheTopOfAnArena) {
    const PathRef first = _trie.append(_arena, PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    _trie.append(_arena, first, EdgeID(11), NodeID(2), 2);
    _trie.append(_arena, first, EdgeID(12), NodeID(3), 2);
    EXPECT_EQ(_trie.getArenaSize(_arena), 3u);

    _trie.truncateArena(_arena, 1);
    EXPECT_EQ(_trie.getArenaSize(_arena), 1u);
    EXPECT_EQ(_trie.size(), 2u);

    const PathRef next = _trie.append(_arena, first, EdgeID(13), NodeID(4), 2);
    EXPECT_EQ(PathTrie::indexOf(next), 1u);

    std::vector<uint64_t> ids;
    readIDs<EdgeID>(_trie.expandEdges(next, _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10, 13}));
}

TEST_F(PathTrieTest, retainChainRewritesItToTheBottom) {
    const PathRef first = _trie.append(_arena, PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    const PathRef second = _trie.append(_arena, first, EdgeID(11), NodeID(2), 2);
    _trie.append(_arena, second, EdgeID(12), NodeID(3), 3);
    const PathRef fourth = _trie.append(_arena, second, EdgeID(13), NodeID(4), 3);

    std::vector<PathRef> chain {PathTrie::ROOT, first, second, fourth};
    _trie.retainChain(_arena, chain);

    EXPECT_EQ(_trie.getArenaSize(_arena), 3u);
    EXPECT_EQ(chain[0], PathTrie::ROOT);
    EXPECT_EQ(PathTrie::indexOf(chain[1]), 0u);
    EXPECT_EQ(PathTrie::indexOf(chain[2]), 1u);
    EXPECT_EQ(PathTrie::indexOf(chain[3]), 2u);
    EXPECT_EQ(_trie.get(chain[1])._parent, PathTrie::ROOT);
    EXPECT_EQ(_trie.get(chain[3])._parent, chain[2]);
    EXPECT_EQ(_trie.getDepth(chain[3]), 3u);

    std::vector<uint64_t> ids;
    readIDs<EdgeID>(_trie.expandEdges(chain[3], _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10, 11, 13}));

    readIDs<NodeID>(_trie.expandSources(chain[3], NodeID(0), _buffer, false), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {0, 1, 2}));

    std::vector<PathRef> empty;
    _trie.retainChain(_arena, empty);
    EXPECT_EQ(_trie.getArenaSize(_arena), 0u);
}
