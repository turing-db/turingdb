#pragma once

#include <unordered_map>

#include "ColumnName.h"

namespace db {

class NamedColumn;

class DataframeHeaderMap {
public:
    DataframeHeaderMap();
    ~DataframeHeaderMap();

    NamedColumn* getColumn(ColumnName name) const {
        const auto colIt = _nameMap.find(name);
        if (colIt == _nameMap.end()) {
            return nullptr;
        }

        return colIt->second;
    }

    void addNameForColumn(NamedColumn* col, ColumnName name) {
        _nameMap[name] = col;
    }

private:
    std::unordered_map<ColumnName, NamedColumn*, ColumnNameHash> _nameMap;
};

}
