#include "HAMTIndex.h"

#include <bit>

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
    return 0;
}

template <typename K, typename V, typename Hash>
PropertyTypeID HAMTIndex<K, V, Hash>::property() const {
    return 0;
}

template <typename K, typename V, typename Hash>
bool HAMTIndex<K, V, Hash>::isNodeIndex() const {
    return false;
}

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::mutInsFrom(HAMTIndexNode* from, size_t depth, const K& key,
                                       const V& value) {

    const bool atMaxDepth = depth == _chunksPerHash;
    if (atMaxDepth) {
        auto* leaf = from->as<HAMTLeaf<K, V>>();
        bioassert(leaf, "Reached max depth, but found not a leaf.");

        leaf->emplace_back(key, value);
        return;
    }

    const bool isLeaf = from->getKind() == HAMTIndexNode::Kind::LEAF;
    bioassert(!isLeaf, "Tried to mut insert at non-max depth leaf.");

    auto* inner = from->as<HAMTInnerNode>();

    const HAMTInnerNode::ChildBitmask childMask = inner->mask();
    const size_t hashcode = _hasher(key);
    const size_t hashChunk = getDepthHash(hashcode, depth);
    const size_t bitIndex = 1UL << hashChunk;
    const size_t chldrnBelowMask = bitIndex - 1;
    const size_t chldrnLesser = childMask & chldrnBelowMask;
    const size_t chldrnLesserCount = std::popcount(chldrnLesser);
}

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::mutableInsert(const K& key, const V& value) {
    HAMTIndexNode* node = _root.get();

    const size_t hashcode = _hasher(key);
    size_t hashChunk = hashcode & _chunkMask;

    size_t depth = 0;

    while (node) {
        const bool isLeaf = node->getKind() == HAMTIndexNode::Kind::LEAF;

        if (isLeaf) {
            const bool reachedMaxDepth = depth == _chunksPerHash;
            auto* leaf = node->as<HAMTLeaf<K, V>>();

            if (reachedMaxDepth) {
                leaf->emplace_back(key, value);
                return;
            }

            bioassert(leaf->_values.size() == 1, "Leaf conflict should be singleton.");
            const auto& other = leaf->_values.front();

            throw FatalException("Found leaf on path to insert");
            // TODO: Handle leaf replacement (if non terminal)/collision resultion (if
            // terminal)
            break;
        }

        auto* inner = node->as<HAMTInnerNode>();

        const HAMTInnerNode::ChildBitmask childMask = inner->mask();

        const size_t bitIndex = 1UL << hashChunk;
        const size_t chldrnBelowMask = bitIndex - 1;
        const size_t chldrnLesser = childMask & chldrnBelowMask;
        const size_t chldrnLesserCount = std::popcount(chldrnLesser);

        const bool exists = (bitIndex & childMask) != 0;

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
const V* HAMTIndex<K, V, Hash>::find(const K& key) {
    HAMTIndexNode* node = _root.get();

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
