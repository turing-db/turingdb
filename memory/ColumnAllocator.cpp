#include "ColumnAllocator.h"

using namespace db;

ColumnAllocatorMap::ColumnAllocatorMap()
{
}

ColumnAllocatorMap::~ColumnAllocatorMap() {
    for (ColumnAllocator* allocator : _allocators) {
        delete allocator;
    }
}
