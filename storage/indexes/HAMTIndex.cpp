#include "HAMTIndex.h"

#include <bit>
#include <stdint.h>

#include "ArcManager.h"

#include "FatalException.h"
#include "indexes/HAMTIndexNode.h"
#include "metadata/PropertyType.h"

using namespace db;

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::init(GraphView view) {
    _root = _man->newInner();
}

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::query(const Column* query, Column* result) const {
    {
        auto* tq = dynamic_cast<const ColumnVector<K>*>(query);
        bioassert(tq, "Invalid query column for index.");
    }
    {
        auto* tr = dynamic_cast<const ColumnVector<V>*>(result);
        bioassert(tr, "Invalid query column for index.");
    }
}

template <typename K, typename V, typename Hash>
size_t HAMTIndex<K, V, Hash>::size() const {
    throw FatalException("HAMTIndex::size() not yet implemented.");
}

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::mutInsFrom(HAMTIndexNode* from, size_t depth, const K& key,
                                       const V& value) {
    {
        // If we have completely exhausted the hashcode of @tparam K, then we are at the
        // maximum depth of the tree. All nodes at this level must be leaves.
        const bool atMaxDepth = depth == _chunksPerHash;
        const bool isLeaf = from->getKind() == HAMTIndexNode::Kind::LEAF;

        if (atMaxDepth) {
            bioassert(isLeaf, "Reached max depth, but found not a leaf.");

            // Full hash collision: insert kv-pair into leaf bucket.
            auto* leaf = from->as<HAMTLeaf<K, V>>();
            bioassert(leaf, "Failed to get leaf.");

            leaf->emplace_back(key, value);
            return;
        }

        // If we are not at max depth, then we must stop recursing on a non-leaf to
        // properly perform node replacement.
        bioassert(!isLeaf, "Tried to mut insert at non-max depth leaf.");
    }

    auto* inner = from->as<HAMTInnerNode>();
    bioassert(inner, "Failed to get inner node.");

    const HashCode hashcode = _hasher(key);
    const HashCode hashChunk = getDepthHash(hashcode, depth);

    const HAMTInnerNode::ChildBitmask childMask = inner->mask();
    const uint64_t bitIndex = 1UL << hashChunk;
    const bool exists = (bitIndex & childMask) != 0;

    const uint64_t chldrnBelowMask = bitIndex - 1;
    const uint64_t chldrnLesser = childMask & chldrnBelowMask;
    const uint8_t chldrnLesserCount = std::popcount(chldrnLesser);

    // If there is no node representing the prefix hash of this traversal, then we may
    // insert a new leaf to represent such a prefix.
    if (!exists) {
        WeakArc<HAMTIndexNode> rc = _man->newLeaf<K, V>();
        auto* newLeaf = rc->as<HAMTLeaf<K, V>>();

        // Add the new key, value pair to the leaf
        newLeaf->emplace_back(key, value);

        // Update the child array and bitmask to contain new leaf
        const auto after = inner->_children.begin() + chldrnLesserCount;
        inner->_children.insert(after, rc);
        inner->_mask |= bitIndex;
        return;
    }

    // Otherwise, a node representing the prefix of this recursive traversal exists.
    WeakArc<HAMTIndexNode> node = inner->children()[chldrnLesserCount];

    const bool isLeaf = node->getKind() == HAMTIndexNode::Kind::LEAF;

    if (!isLeaf) {
        // FIXME: return the inserted value to update @ref inner 's child array + bitmask
        mutInsFrom(node.get(), depth + 1, key, value);
        return;
    }

    // Else: leaf node -> convert to inner node with 2 children
    // TODO: Implement leaf -> inner conversion with 2 children 

    auto* oldLeaf = node->as<HAMTLeaf<K, V>>();
    bioassert(oldLeaf, "Failed to get old leaf.");

    WeakArc<HAMTIndexNode> rc = _man->newInner();
    [[maybe_unused]] auto* replacementNode = rc->as<HAMTInnerNode>();
}

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::mutableInsert(const K& key, const V& value) {
    HAMTIndexNode* node = _root.get();

    const HashCode hashcode = _hasher(key);
    HashCode hashChunk = hashcode & _chunkMask;

    size_t depth = 0;

    while (node) {
        const bool isLeaf = node->getKind() == HAMTIndexNode::Kind::LEAF;

        if (isLeaf) {
            const bool reachedMaxDepth = depth == _chunksPerHash;
            auto* leaf = node->as<HAMTLeaf<K, V>>();

            // We have traversed the tree down to the maximum depth, i.e. we have
            // exhausted the entire hashcode of a @tparam K. This is a total hash
            // collision: insert our kv pair into the leaf array for linear search.
            if (reachedMaxDepth) {
                leaf->emplace_back(key, value);
                return;
            }

            // If we have not reached the max depth, this should be the first time the
            // proper prefix of the hash causes a collision, i.e. there should only be a
            // single value in the leaf kv-pair storage.
            bioassert(leaf->_values.size() == 1, "Leaf conflict should be singleton.");

            // Since we are not at max depth, we replace this leaf with a
            // @ref HAMTInnerNode, with both the already existing @ref leaf and a new
            // @ref HAMTLeaf containing the kv-pair to insert.
            [[maybe_unused]] const auto& other = leaf->_values.front();

            throw FatalException("Found leaf on path to insert");
            // TODO: Handle leaf replacement (if non terminal)/collision resultion (if
            // terminal)
            break;
        }

        // If this is not a leaf, we need to traverse the tree deeper
        auto* inner = node->as<HAMTInnerNode>();

        const HAMTInnerNode::ChildBitmask childMask = inner->mask();
        const uint64_t bitIndex = 1UL << hashChunk;
        const bool exists = (bitIndex & childMask) != 0;

        const uint64_t chldrnBelowMask = bitIndex - 1;
        const uint64_t chldrnLesser = childMask & chldrnBelowMask;
        const uint8_t chldrnLesserCount = std::popcount(chldrnLesser);

        if (exists) {
            HAMTInnerNode::Children& chldrn = inner->_children;
            node = chldrn[chldrnLesserCount].get();
            depth++;
            hashChunk = (hashcode >> depth * _hashChunkSize) & _chunkMask;
            continue;
        }

        // Doesn't exist: add this child as a leaf
        WeakArc<HAMTIndexNode> rc = _man->newLeaf<K, V>();

        { // Store the value for the inserted entry
            auto* leaf = rc->as<HAMTLeaf<K, V>>();
            leaf->emplace_back(key, value);
        }

        { // Update parent's child array and child bitmask
            const auto after = inner->_children.begin() + chldrnLesserCount;
            inner->_children.insert(after, rc);
            inner->_mask |= bitIndex;
        }

        return;
    }
}

template <typename K, typename V, typename Hash>
const V* HAMTIndex<K, V, Hash>::find(const K& key) const {
    const HAMTIndexNode* cnode = _root.get();
    HAMTIndexNode* node = const_cast<HAMTIndexNode*>(cnode);

    const size_t hashcode = _hasher(key);
    size_t hashChunk = hashcode & _chunkMask;

    size_t depth = 0;
    while (node) {
        const bool isLeaf = node->getKind() == HAMTIndexNode::Kind::LEAF;

        if (isLeaf) {
            auto* leaf = node->as<HAMTLeaf<K, V>>();
            auto& pairs = leaf->_values;
            for (const auto& [k, v] : pairs) {
                if (k == key) {
                    return &v;
                }
            }
            return nullptr;
        }

        auto* inner = node->as<HAMTInnerNode>();

        const HAMTInnerNode::ChildBitmask childMask = inner->mask();
        const size_t bitIndex = 1UL << hashChunk;

        const bool exists = (bitIndex & childMask) != 0;
        if (!exists) {
            return nullptr;
        }

        const size_t chldrnBelowMask = bitIndex - 1;
        const size_t chldrnLesser = childMask & chldrnBelowMask;
        const size_t chldrnLesserCount = std::popcount(chldrnLesser);

        HAMTInnerNode::Children& chldrn = inner->_children;
        node = chldrn[chldrnLesserCount].get();
        depth++;
        hashChunk = (hashcode >> depth * _hashChunkSize) & _chunkMask;
    }

    return nullptr;
}

namespace db {
template class HAMTIndex<types::String::Primitive, NodeID>;
}
