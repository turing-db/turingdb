#include "ColumnNameManager.h"

#include "ColumnName.h"

using namespace db;

ColumnNameManager::ColumnNameManager()
{
}

ColumnNameManager::~ColumnNameManager() {
    for (ColumnNameSharedString* sharedStr : _strings) {
        delete sharedStr;
    }
}

ColumnNameSharedString* ColumnNameManager::getString(std::string_view str) {
    const std::string sstr(str);
    const auto sharedStrIt = _strMap.find(sstr);
    if (sharedStrIt != _strMap.end()) {
        return sharedStrIt->second;
    }

    ColumnNameSharedString* sharedStr = new ColumnNameSharedString(std::string(str));
    _strings.push_back(sharedStr);
    _strMap[std::move(sstr)] = sharedStr;
    return sharedStr;
}
