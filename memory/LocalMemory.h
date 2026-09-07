#pragma once

#include <string_view>
#include <stddef.h>

#include "MemoryPool.h"
#include "TypeValueMap.h"

#include "ColumnAllocator.h"
#include "columns/ColumnSet.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnStringTable.h"

#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "list/PathTrie.h"

#include "map/MapBuffer.h"
#include "map/MapView.h"

#include "buffers/SpanBuffer.h"
#include "buffers/StringBuffer.h"

#include "metadata/PropertyType.h"
#include "metadata/PropertyNull.h"
#include "ID.h"
#include "versioning/ChangeID.h"

namespace db {

class CommitBuilder;
class Change;

class LocalMemory {
public:
    using DefaultMapBuffer = MapBuffer<>;
    using EmbeddingBuffer = SpanBuffer<float, types::Embedding::Primitive>;

    template <typename T>
    struct MakeMemoryPool {
        using type = TypeValueMapPair<T, MemoryPool<T>>;
    };

    using MemoryPools = TypeValueMap<
        MakeMemoryPool<ColumnVector<EntityID>>::type,
        MakeMemoryPool<ColumnVector<NodeID>>::type,
        MakeMemoryPool<ColumnVector<EdgeID>>::type,
        MakeMemoryPool<ColumnVector<PathRef>>::type,
        MakeMemoryPool<ColumnVector<Path>>::type,
        MakeMemoryPool<ColumnVector<EntityList>>::type,
        MakeMemoryPool<ColumnVector<LabelID>>::type,
        MakeMemoryPool<ColumnVector<LabelSetID>>::type,
        MakeMemoryPool<ColumnVector<EdgeTypeID>>::type,
        MakeMemoryPool<ColumnVector<PropertyTypeID>>::type,
        MakeMemoryPool<ColumnVector<PropertyType>>::type,
        MakeMemoryPool<ColumnVector<ChangeID>>::type,
        MakeMemoryPool<ColumnVector<ValueType>>::type,
        MakeMemoryPool<ColumnVector<size_t>>::type,
        MakeMemoryPool<ColumnVector<std::string_view>>::type,
        MakeMemoryPool<ColumnVector<std::string>>::type,
        MakeMemoryPool<ColumnMask>::type,
        MakeMemoryPool<ColumnConst<NodeID>>::type,
        MakeMemoryPool<ColumnConst<EdgeID>>::type,
        MakeMemoryPool<ColumnConst<LabelSetID>>::type,
        MakeMemoryPool<ColumnConst<EdgeTypeID>>::type,
        MakeMemoryPool<ColumnConst<size_t>>::type,

        MakeMemoryPool<ColumnConst<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::UInt64::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::Double::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::String::Primitive>>::type,
        MakeMemoryPool<ColumnConst<std::string>>::type,
        MakeMemoryPool<ColumnConst<types::Bool::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::Embedding::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::DateTime::Primitive>>::type,

        MakeMemoryPool<ColumnConst<std::optional<types::Int64::Primitive>>>::type,
        MakeMemoryPool<ColumnConst<std::optional<types::Int64::Primitive>>>::type,
        MakeMemoryPool<ColumnConst<std::optional<types::UInt64::Primitive>>>::type,
        MakeMemoryPool<ColumnConst<std::optional<types::Double::Primitive>>>::type,
        MakeMemoryPool<ColumnConst<std::optional<types::String::Primitive>>>::type,
        MakeMemoryPool<ColumnConst<std::optional<std::string>>>::type,
        MakeMemoryPool<ColumnConst<std::optional<types::Bool::Primitive>>>::type,
        MakeMemoryPool<ColumnConst<std::optional<types::Embedding::Primitive>>>::type,
        MakeMemoryPool<ColumnConst<std::optional<types::DateTime::Primitive>>>::type,

        MakeMemoryPool<ColumnConst<PropertyNull>>::type,

        MakeMemoryPool<ColumnVector<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::UInt64::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::Double::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::String::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::Bool::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::Embedding::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::DateTime::Primitive>>::type,

        MakeMemoryPool<ColumnVector<const CommitBuilder*>>::type,
        MakeMemoryPool<ColumnVector<const Change*>>::type,
        MakeMemoryPool<ColumnOptVector<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::UInt64::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::Double::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::String::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::Bool::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<std::string>>::type,
        MakeMemoryPool<ColumnOptVector<types::Embedding::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::DateTime::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<NodeID>>::type,
        MakeMemoryPool<ColumnOptVector<EdgeID>>::type,
        MakeMemoryPool<ColumnOptVector<EdgeTypeID>>::type,
        MakeMemoryPool<ColumnOptVector<LabelID>>::type,
        MakeMemoryPool<ColumnOptVector<PropertyTypeID>>::type,
        MakeMemoryPool<ColumnOptVector<ValueType>>::type,
        MakeMemoryPool<ColumnSet<NodeID>>::type,
        MakeMemoryPool<ColumnSet<EdgeID>>::type,
        MakeMemoryPool<ColumnStringTable>::type,

        MakeMemoryPool<ColumnVector<ListView>>::type,
        MakeMemoryPool<ColumnOptVector<ListView>>::type,
        MakeMemoryPool<ColumnConst<ListView>>::type,
        MakeMemoryPool<ColumnVector<ListElementView>>::type,
        MakeMemoryPool<ColumnOptVector<ListElementView>>::type,
        MakeMemoryPool<ColumnConst<ListElementView>>::type,
        MakeMemoryPool<ColumnConst<std::optional<ListElementView>>>::type,
        MakeMemoryPool<ColumnOptVector<ListView>>::type,
        MakeMemoryPool<ColumnConst<std::optional<ListView>>>::type,

        MakeMemoryPool<ColumnVector<MapView>>::type,
        MakeMemoryPool<ColumnOptVector<MapView>>::type,
        MakeMemoryPool<ColumnConst<MapView>>::type,
        MakeMemoryPool<ColumnConst<std::optional<MapView>>>::type
    >;

    template <typename KeyT, typename ValueT>
    struct ClearTransform {
        void operator()(ValueT& value) const {
            value.clear();
        }
    };

    LocalMemory(const LocalMemory&) = delete;
    LocalMemory(LocalMemory&&) = delete;
    LocalMemory& operator=(const LocalMemory&) = delete;
    LocalMemory& operator=(LocalMemory&&) = delete;

    LocalMemory();
    ~LocalMemory();

    template <typename ObjT, typename... Args>
    ObjT* alloc(Args&&... args) {
        return _pools.get<ObjT>().alloc(args...);
    }

    Column* allocSame(const Column* col) {
        return _columnAllocators.get(col->getKind())->alloc();
    }

    void clear() {
        _pools.transform<ClearTransform>();
        _listBuffer.clear();
        _mapBuffer.clear();
        _pathTrie.clear();
        _stringBuf.clear();
        _embeddingBuf.clear();
    }

    QueryListBuffer& listBuffer() { return _listBuffer; }

    DefaultMapBuffer& mapBuffer() { return _mapBuffer; }
    PathTrie& pathTrie() { return _pathTrie; }

    StringBuffer& stringBuffer() { return _stringBuf; }

    EmbeddingBuffer& embeddingBuffer() { return _embeddingBuf; }

private:
    MemoryPools _pools;
    ColumnAllocatorMap _columnAllocators;

    QueryListBuffer _listBuffer;
    DefaultMapBuffer _mapBuffer;
    PathTrie _pathTrie;
    StringBuffer _stringBuf;
    EmbeddingBuffer _embeddingBuf;
};

}
