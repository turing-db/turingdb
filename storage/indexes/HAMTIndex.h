#pragma once

#include "Index.h"

#include "HAMTIndexManager.h"
#include "HAMTIndexNode.h"

#include "ArcManager.h"

namespace db {

class Column;

template <typename K, typename V, typename Hash = std::hash<K>>
class HAMTIndex final : public Index {
public:
    HAMTIndex(std::string_view name, HAMTManager* man, PropertyTypeID pid)
        : Index(name),
        _man(man),
        _propID(pid)
    {
    }

    void init(GraphView view) final;

    void insert(const Column* keys, const Column* values);

    void query(const Column* query, Column* result) const final;

    void boundedQuery(const Column* query,
                      Column* result,
                      ColumnIndices* indices,
                      QueryState& state,
                      size_t limit) const final {}

    size_t size() const final;

    PropertyTypeID property() const final;

    consteval bool isNodeIndex() const final;

    /**
     * @brief Inserts a value into the hash tree, without preserving immutable copies of
     * the prior state of nodes.
     * @detail Used in calls to @ref init, where there is no prior state to keep immutable
     */
    void mutableInsert(const K& key, const V& value);
    const V* find(const K& key) const;

private:
    WeakArc<HAMTIndexNode> _root;

    Hash _hasher;

    // TODO Decide whether the manager should be per index or per graph/db
    HAMTManager* _man {nullptr};

    PropertyTypeID _propID;

    static constexpr bool _isNode = std::is_same_v<V, NodeID>;

    /// Split each hash into chunks of 4 bits
    static constexpr size_t _hashChunkSize = 4;
    static constexpr size_t _chunksPerHash = (sizeof(size_t) * 8) / _hashChunkSize;
    static_assert(_chunksPerHash == 16);
    /// Mask of 0b1111 for the initial chunk mask
    static constexpr size_t _chunkMask = (1UL << (_hashChunkSize)) - 1;

    size_t getDepthHash(size_t hashcode, size_t depth) {
        return (hashcode >> depth * _hashChunkSize) & _chunkMask;
    }

    void mutInsFrom(HAMTIndexNode* from, size_t depth, const K& key, const V& value);

    static_assert((sizeof(size_t) * 8) % _hashChunkSize == 0, "Chunking assumption.");
    static_assert(sizeof(size_t) == 8, "Chunking assumption violated.");
    static_assert(_isNode or std::is_same_v<V, EdgeID>, "Non-entity ID index.");
};
}
