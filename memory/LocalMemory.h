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

#include "metadata/PropertyType.h"
#include "metadata/PropertyNull.h"
#include "ID.h"
#include "versioning/ChangeID.h"

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
        MakeMemoryPool<ColumnVector<Path>>::type,
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
        MakeMemoryPool<ColumnConst<types::Bool::Primitive>>::type,

        MakeMemoryPool<ColumnConst<PropertyNull>>::type,

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
        MakeMemoryPool<ColumnOptVector<types::Bool::Primitive>>::type,
        MakeMemoryPool<ColumnSet<NodeID>>::type,
        MakeMemoryPool<ColumnSet<EdgeID>>::type>;

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
    }

private:
    MemoryPools _pools;
    ColumnAllocatorMap _columnAllocators;
};

}
