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
    // TODO: Use @param view to mutable insert all relevant kv-pairs
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
const V* HAMTIndex<K, V, Hash>::find(const K& key) const {
    const HAMTIndexNode* node = _root.get();

    const size_t hashcode = _hasher(key);

    ssize_t depth = -1;
    while (node) {
        depth++;
        const size_t hashChunk = getDepthHash(hashcode, depth);

        const bool isLeaf = node->getKind() == HAMTIndexNode::Kind::LEAF;

        if (isLeaf) {
            const auto* leaf = node->as<const HAMTLeaf<K, V>>();
            auto& pairs = leaf->_values;
            for (const auto& [k, v] : pairs) {
                if (k == key) {
                    return &v;
                }
            }
            return nullptr;
        }

        const auto* inner = node->as<HAMTInnerNode>();

        const HAMTInnerNode::ChildBitmask childMask = inner->mask();
        const size_t bitIndex = 1UL << hashChunk;

        const bool exists = (bitIndex & childMask) != 0;
        if (!exists) {
            return nullptr;
        }

        const size_t chldrnBelowMask = bitIndex - 1;
        const size_t chldrnLesser = childMask & chldrnBelowMask;
        const size_t chldrnLesserCount = std::popcount(chldrnLesser);

        const HAMTInnerNode::Children& chldrn = inner->children();
        node = chldrn[chldrnLesserCount].get();
    }

    return nullptr;
}

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::exhaustiveMutInsert(const K& key, const V& value) {
    HAMTIndexNode* node = _root.get();

    const HashCode hashcode = _hasher(key);

    ssize_t depth = -1;
    while (node) {
        depth++;
        const HashCode hashChunk = getDepthHash(hashcode, depth);

        const bool isLeaf = node->getKind() == HAMTIndexNode::Kind::LEAF;
        const bool atMaxDepth = depth == _chunksPerHash;

        if (isLeaf) {
            bioassert(atMaxDepth, "Reached non-max-depth leaf.");

            auto* leaf = node->as<HAMTLeaf<K, V>>();
            leaf->emplace_back(key, value);
            return;
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
            continue;
        }

        const bool penultimateDepth = depth == _chunksPerHash - 1;

        // Create a leaf
        if (penultimateDepth) {
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

        // Otherwise create a new inner node and recurse
        WeakArc<HAMTIndexNode> rc = _man->newInner();

        { // Update parent's child array and child bitmask
            const auto after = inner->_children.begin() + chldrnLesserCount;
            inner->_children.insert(after, rc);
            inner->_mask |= bitIndex;
        }

        node = rc.get();
    }
}

template <typename K, typename V, typename Hash>
size_t HAMTIndex<K, V, Hash>::computeChildIndex(const HAMTInnerNode* parent, HashCode hashChunk) {
    const HAMTInnerNode::ChildBitmask childMask = parent->mask();
    const uint64_t bitIndex = 1UL << hashChunk;

    const uint64_t chldrnBelowMask = bitIndex - 1;
    const uint64_t chldrnLesser = childMask & chldrnBelowMask;
    const uint8_t chldrnLesserCount = std::popcount(chldrnLesser);

    return chldrnLesserCount;
}

template <typename K, typename V, typename Hash>
HAMTInnerNode* HAMTIndex<K, V, Hash>::newInnerChild(HAMTInnerNode* parent, HashCode hashChunk) {
    const size_t childIndex = computeChildIndex(parent, hashChunk);
    // WARN: Does not check that the child does not exist already

    WeakArc<HAMTIndexNode> rc = _man->newInner();
    HAMTInnerNode* ptr = rc->as<HAMTInnerNode>();

    { // Update parent's child array and child bitmask
        const auto after = parent->_children.begin() + childIndex;
        parent->_children.insert(after, rc);
        parent->_mask |= bitIndex;
    }

    return ptr;
}

template <typename K, typename V, typename Hash>
HAMTLeaf<K, V>* HAMTIndex<K, V, Hash>::newLeafChild(HAMTInnerNode* parent, HashCode hashChunk) {
    const size_t childIndex = computeChildIndex(parent, hashChunk);
    // WARN: Does not check that the child does not exist already

    WeakArc<HAMTIndexNode> rc = _man->newLeaf<K, V>();
    HAMTLeaf<K, V>* ptr = rc->as<HAMTLeaf<K,V>>();

    { // Update parent's child array and child bitmask
        const auto after = parent->_children.begin() + childIndex;
        parent->_children.insert(after, rc);
        parent->_mask |= bitIndex;
    }

    return ptr;
}

namespace db {
template class HAMTIndex<types::String::Primitive, NodeID>;
template class HAMTIndex<types::UInt64::Primitive, NodeID>;
}
