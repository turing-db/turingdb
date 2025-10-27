#pragma once

#include <unordered_map>

#include "ColumnName.h"

namespace db {

class NamedColumn;
class Dataframe;

class DataframeHeaderMap {
public:
    friend Dataframe;

    DataframeHeaderMap();
    ~DataframeHeaderMap();

    NamedColumn* getColumn(ColumnName name) const {
        const auto colIt = _nameMap.find(name);
        if (colIt == _nameMap.end()) {
            return nullptr;
        }

        return colIt->second;
    }

private:
    std::unordered_map<ColumnName, NamedColumn*, ColumnNameHash> _nameMap;

    void addNameForColumn(NamedColumn* col, ColumnName name) {
        _nameMap[name] = col;
    }

    void removeColumnName(ColumnName name) {
        _nameMap.erase(name);
    }
};

}
