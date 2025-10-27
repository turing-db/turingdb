#pragma once

#include <vector>
#include <string_view>
#include <string>
#include <unordered_map>

#include "ColumnName.h"

namespace db {

class ColumnNameSharedString;

class ColumnNameManager {
public:
    ColumnNameManager();
    ~ColumnNameManager();

    ColumnName getName(std::string_view str) {
        return ColumnName(getString(str));
    }

    ColumnNameSharedString* getString(std::string_view str);

private:
    std::vector<ColumnNameSharedString*> _strings;
    std::unordered_map<std::string, ColumnNameSharedString*> _strMap;
};

}
