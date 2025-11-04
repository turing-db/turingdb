#pragma once

#include <vector>
#include <unordered_map>
#include <functional>

#include "columns/ColumnKind.h"

namespace db {

class Column;

class ColumnAllocator {
public:
    using AllocFunc = std::function<Column*()>;

    ColumnAllocator(const AllocFunc& allocFunc)
        : _allocFunc(allocFunc)
    {
    }

    ~ColumnAllocator() = default;

    Column* alloc() { return _allocFunc(); }

private:
    AllocFunc _allocFunc;
};

class ColumnAllocatorMap {
public:
    ColumnAllocatorMap();
    ~ColumnAllocatorMap();

    ColumnAllocator* get(ColumnKind::ColumnKindCode code) const {
        return _allocMap.at(code);
    }

    void add(ColumnKind::ColumnKindCode code, ColumnAllocator* allocator) {
        _allocators.push_back(allocator);
        _allocMap[code] = allocator;
    }

private:
    std::vector<ColumnAllocator*> _allocators;
    std::unordered_map<ColumnKind::ColumnKindCode, ColumnAllocator*> _allocMap;
};

}
