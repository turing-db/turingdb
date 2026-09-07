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
    template <typename IDType>
    void readIDs(const ListView& list, std::vector<uint64_t>& ids) {
        ids.clear();
        for (const ListElementView& element : list) {
            ids.push_back(element.getAs<IDType>().getValue());
        }
    }

    PathTrie _trie;
    ListBuffer<> _buffer;
};

}

TEST_F(PathTrieTest, rootIsTheEmptyPath) {
    EXPECT_EQ(_trie.size(), 1u);
    EXPECT_EQ(_trie.getDepth(PathTrie::ROOT), 0u);

    std::vector<uint64_t> ids;

    readIDs<EdgeID>(_trie.expandEdges(PathTrie::ROOT, _buffer), ids);
    EXPECT_TRUE(ids.empty());

    readIDs<NodeID>(_trie.expandEnds(PathTrie::ROOT, _buffer), ids);
    EXPECT_TRUE(ids.empty());

    readIDs<NodeID>(_trie.expandSources(PathTrie::ROOT, NodeID(7), _buffer), ids);
    EXPECT_TRUE(ids.empty());
}

TEST_F(PathTrieTest, expandsAChainRootFirst) {
    const PathRef first = _trie.append(PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    const PathRef second = _trie.append(first, EdgeID(11), NodeID(2), 2);
    const PathRef third = _trie.append(second, EdgeID(12), NodeID(3), 3);

    EXPECT_EQ(_trie.getDepth(third), 3u);
    EXPECT_EQ(_trie.get(third)._parent, second);
    EXPECT_EQ(_trie.get(second)._parent, first);
    EXPECT_EQ(_trie.get(first)._parent, PathTrie::ROOT);

    std::vector<uint64_t> ids;

    readIDs<EdgeID>(_trie.expandEdges(third, _buffer), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10, 11, 12}));

    readIDs<NodeID>(_trie.expandEnds(third, _buffer), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {1, 2, 3}));

    readIDs<NodeID>(_trie.expandSources(third, NodeID(0), _buffer), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {0, 1, 2}));

    readIDs<EdgeID>(_trie.expandEdges(first, _buffer), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10}));

    readIDs<NodeID>(_trie.expandSources(first, NodeID(0), _buffer), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {0}));
}

TEST_F(PathTrieTest, sharedPrefixIsStoredOnce) {
    const PathRef prefix = _trie.append(PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    const PathRef left = _trie.append(prefix, EdgeID(11), NodeID(2), 2);
    const PathRef right = _trie.append(prefix, EdgeID(12), NodeID(3), 2);

    // The root, the prefix and the two leaves: the prefix is not repeated per path
    EXPECT_EQ(_trie.size(), 4u);

    std::vector<uint64_t> ids;

    readIDs<EdgeID>(_trie.expandEdges(left, _buffer), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10, 11}));

    readIDs<EdgeID>(_trie.expandEdges(right, _buffer), ids);
    EXPECT_EQ(ids, (std::vector<uint64_t> {10, 12}));
}

TEST_F(PathTrieTest, clearKeepsOnlyTheRoot) {
    _trie.append(PathTrie::ROOT, EdgeID(10), NodeID(1), 1);
    _trie.clear();

    EXPECT_EQ(_trie.size(), 1u);
    EXPECT_EQ(_trie.getDepth(PathTrie::ROOT), 0u);

    const PathRef first = _trie.append(PathTrie::ROOT, EdgeID(20), NodeID(5), 1);
    EXPECT_EQ(first, PathRef(1));
}
