#pragma once

#include <string_view>

#include "ColumnTag.h"

#include "DynamicLookupTable.h"

namespace db {

class DataframeManager;

class ColumnTagManager {
public:
    friend DataframeManager;

    ColumnTag allocTag() {
        const ColumnTag tag = _nextFreeTag;
        _nextFreeTag++;
        return tag;
    }

    std::string_view getName(ColumnTag tag) const { return _nameMap.lookup(tag.getValue()); }

    void setTagName(ColumnTag tag, std::string_view name) {
        _nameMap.insert(tag.getValue(), name);
    }

private:
    ColumnTag _nextFreeTag {0};
    DynamicLookupTable<std::string_view> _nameMap;

    ColumnTagManager() = default;
    ~ColumnTagManager() = default;
};

}
