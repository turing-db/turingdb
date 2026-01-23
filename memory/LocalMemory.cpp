#include "LocalMemory.h"

#include "columns/Column.h"
#include "columns/ColumnSet.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"

using namespace db;

template <typename KeyT, typename ValueT>
struct RegisterColumnAllocator {
    ColumnAllocatorMap& _columnAllocators;

    RegisterColumnAllocator(ColumnAllocatorMap& columnAllocators)
        : _columnAllocators(columnAllocators)
    {
    }

    void operator()(ValueT& value) const {
        const ColumnAllocator::AllocFunc lambda = [&value]() -> db::Column* { return value.alloc(); };
        _columnAllocators.add(KeyT::staticKind(), new ColumnAllocator(lambda));
    }
};

LocalMemory::LocalMemory()
{
    _pools.transform<RegisterColumnAllocator>(_columnAllocators);
}

LocalMemory::~LocalMemory() {
}
