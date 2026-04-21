#include "HAMTIndex.h"
#include "indexes/HAMTIndexNode.h"
#include "metadata/PropertyType.h"

using namespace db;

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::init(GraphView view) {
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
void HAMTIndex<K, V, Hash>::mutableInsert(const K& key, const V& value) {
    const size_t hashcode = _hasher(key);
    size_t mask = _initialChunkMask;

    [[maybe_unused]] size_t hashChunk = hashcode & mask;

    WeakArc<HAMTIndexNode> node = _root;

    [[maybe_unused]] const auto tryGetChild = [](size_t hashChunk) -> WeakArc<HAMTIndexNode> {
        return {};
    };
}

namespace db {
template class HAMTIndex<types::String::Primitive, NodeID>;
}
