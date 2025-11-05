#include "LocalMemory.h"

using namespace db;

template <typename KeyT, typename ValueT>
struct RegisterColumnAllocator {
    ColumnAllocatorMap& _columnAllocators;

    RegisterColumnAllocator(ColumnAllocatorMap& columnAllocators)
        : _columnAllocators(columnAllocators)
    {
    }

    void operator()(ValueT& value) const {
        if constexpr (std::is_same_v<KeyT, NamedColumn>) {
            // Do not register in column allocator
        } else {
            auto lambda = [&value]() { return value.alloc(); };
            _columnAllocators.add(KeyT::staticKind(), new ColumnAllocator(lambda));
        }
    }
};

LocalMemory::LocalMemory()
{
    _pools.transform<RegisterColumnAllocator>(_columnAllocators);
}

LocalMemory::~LocalMemory() {
}
