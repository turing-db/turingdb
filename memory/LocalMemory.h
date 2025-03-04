#pragma once

#include <string_view>
#include <stddef.h>

#include "MemoryPool.h"
#include "TypeValueMap.h"

#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOptVector.h"
#include "types/PropertyType.h"
#include "EntityID.h"

namespace db {

class LocalMemory {
public:
    template <typename T>
    struct MakeMemoryPool {
        using type = TypeValueMapPair<T, MemoryPool<T>>;
    };

    using MemoryPools = TypeValueMap<
        MakeMemoryPool<ColumnVector<EntityID>>::type,
        MakeMemoryPool<ColumnVector<LabelSetID>>::type,
        MakeMemoryPool<ColumnVector<PropertyType>>::type,
        MakeMemoryPool<ColumnVector<size_t>>::type,
        MakeMemoryPool<ColumnVector<std::string_view>>::type,
        MakeMemoryPool<ColumnMask>::type,
        MakeMemoryPool<ColumnConst<LabelSetID>>::type,
        MakeMemoryPool<ColumnConst<EdgeTypeID>>::type,
        MakeMemoryPool<ColumnVector<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::UInt64::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::Double::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::String::Primitive>>::type,
        MakeMemoryPool<ColumnVector<types::Bool::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::Int64::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::UInt64::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::Double::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::String::Primitive>>::type,
        MakeMemoryPool<ColumnOptVector<types::Bool::Primitive>>::type
    >;

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
        _pools.get<ColumnVector<PropertyType>>().clear();
        _pools.get<ColumnVector<size_t>>().clear();
        _pools.get<ColumnConst<EdgeTypeID>>().clear();
    }

private:
    MemoryPools _pools;
};

}
