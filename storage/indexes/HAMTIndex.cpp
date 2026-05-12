#include "HAMTIndex.h"

#include <algorithm>
#include <bit>
#include <stdint.h>

#include <range/v3/numeric/iota.hpp>
#include <range/v3/action/sort.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/to_container.hpp>

#include "ArcManager.h"

#include "FatalException.h"
#include "indexes/HAMTIndexNode.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

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
    WeakArc<HAMTIndexNode> rc = _man->newInner();
    HAMTInnerNode* ptr = rc->as<HAMTInnerNode>();
    parent->insertChild(hashChunk, rc);
    return ptr;
}

template <typename K, typename V, typename Hash>
HAMTLeaf<K, V>* HAMTIndex<K, V, Hash>::newLeafChild(HAMTInnerNode* parent, HashCode hashChunk) {
    WeakArc<HAMTIndexNode> rc = _man->newLeaf<K, V>();
    HAMTLeaf<K, V>* ptr = rc->as<HAMTLeaf<K, V>>();
    parent->insertChild(hashChunk, rc);
    return ptr;
}

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::build(std::span<const K> keys, std::span<const V> values) {
    // 1. sort hashes of keys

    bioassert(keys.size() == values.size(), "Mistmatched key-value dimensions.");
    const size_t n = keys.size();
    std::vector<size_t> indices(n);
    rg::iota(indices, 0UL);

    // lazy: we are not doing multiple iterations over this view
    const auto hashedKeys = keys | rv::transform(std::hash<K> {});

    const auto cmp = [&hashedKeys](size_t i, size_t j) {
        return hashedKeys[i] < hashedKeys[j];
    };
    rg::sort(indices, cmp);

    // materialised: we are doing multiple iterations over this range
    const std::vector<HashCode> sortedHashes =
        indices
        | rv::transform([&hashedKeys](size_t i) -> HashCode { return hashedKeys[i]; })
        | rg::to<std::vector>();

    const auto sortedKeys =
        indices
        | rv::transform([&keys](size_t i) -> const K& { return keys[i]; });

    // lazy generate the sorted values: only iter through once
    /*const auto sortedValues =
        indices | rv::transform([&values](size_t i) -> const V& { return values[i]; });*/


    const auto different = [](HashCode a, HashCode b) { return a != b; };
    auto it = begin(sortedHashes);
    const auto endSentinel = end(sortedHashes);

    do {
        const auto rgEnd = std::adjacent_find(it, endSentinel, different);
        // keys in this subrange share the same node
        const auto same = std::ranges::subrange(it, rgEnd);
    } while (it != endSentinel);
}

namespace db {
template class HAMTIndex<types::String::Primitive, NodeID>;
template class HAMTIndex<types::UInt64::Primitive, NodeID>;
}
