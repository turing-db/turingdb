#pragma once

#include <string_view>
#include <stddef.h>

#include "MemoryPool.h"
#include "TypeValueMap.h"

#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "ID.h"

namespace db {

class CommitBuilder;
class Change;

class LocalMemory {
public:
    template <typename T>
    struct MakeMemoryPool {
        using type = TypeValueMapPair<T, MemoryPool<T>>;
    };

    using MemoryPools = TypeValueMap<
        MakeMemoryPool<ColumnVector<EntityID>>::type,
        MakeMemoryPool<ColumnVector<NodeID>>::type,
        MakeMemoryPool<ColumnVector<EdgeID>>::type,
        MakeMemoryPool<ColumnVector<LabelSetID>>::type,
        MakeMemoryPool<ColumnVector<PropertyType>>::type,
        MakeMemoryPool<ColumnVector<size_t>>::type,
        MakeMemoryPool<ColumnVector<std::string_view>>::type,
        MakeMemoryPool<ColumnVector<std::string>>::type,
        MakeMemoryPool<ColumnMask>::type,
        MakeMemoryPool<ColumnConst<NodeID>>::type,
        MakeMemoryPool<ColumnConst<EdgeID>>::type,
        MakeMemoryPool<ColumnConst<LabelSetID>>::type,
        MakeMemoryPool<ColumnConst<EdgeTypeID>>::type,
        MakeMemoryPool<ColumnConst<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::UInt64::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::Double::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::String::Primitive>>::type,
        MakeMemoryPool<ColumnConst<types::Bool::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::UInt64::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::Double::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::String::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::Bool::Primitive>>::type,
        MakeMemoryPool<ColumnVector<const CommitBuilder*>>::type,
        MakeMemoryPool<ColumnVector<const Change*>>::type,
        MakeMemoryPool<ColumnOptVector<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::UInt64::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::Double::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::String::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::Bool::Primitive>>::type>;

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

    void clear() {
        _pools.get<ColumnVector<EntityID>>().clear();
        _pools.get<ColumnVector<NodeID>>().clear();
        _pools.get<ColumnVector<EdgeID>>().clear();
        _pools.get<ColumnVector<PropertyType>>().clear();
        _pools.get<ColumnVector<size_t>>().clear();
        _pools.get<ColumnConst<EdgeTypeID>>().clear();
        _pools.get<ColumnVector<std::string>>().clear();
        _pools.get<ColumnVector<std::string_view>>().clear();
        _pools.get<ColumnVector<const CommitBuilder*>>().clear();
        _pools.get<ColumnVector<const Change*>>().clear();
    }

private:
    MemoryPools _pools;
};

}
