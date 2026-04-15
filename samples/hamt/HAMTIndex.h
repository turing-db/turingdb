#pragma once

#include <vector>

#include "ArcManager.h"

namespace db {

template <typename K, typename V>
class HAMTManager;
template <typename K, typename V>
class HAMTNode;

template<typename K, typename V, typename Hash = std::hash<K>>
class HAMTIndex {
class HAMTLeaf;

public:
    using Node = HAMTNode<K, V>;

    void insert(const K& key, const V& val);

    void query(const K& key);

private:
    WeakArc<Node> _root;
    Hash _hasher;

    HAMTManager<K, V>* _man;

    /// Split each hash into chunks of 4 bits
    static constexpr size_t _hashChunkSize = 4;
    static constexpr size_t _chunksPerHash = (sizeof(size_t) * 8) / _hashChunkSize;
    /// Mask of 0b1111 for the initial chunk mask
    static constexpr size_t _initialChunkMask = (1UL << (_hashChunkSize + 1)) - 1;

    /// Helper to move the mask along to the next chunk
    static constexpr auto nextMask = [](size_t mask) constexpr -> size_t {
        return mask << _hashChunkSize;
    };

    static_assert((sizeof(size_t) * 8) % _hashChunkSize == 0);
    static_assert(sizeof(size_t) == 8);
};

template <typename K, typename V, typename Hash>
class HAMTIndex<K, V, Hash>::HAMTLeaf {
public:
    const std::vector<V>& values() const { return _values; }

private:
    std::vector<V> _values;
};

}
