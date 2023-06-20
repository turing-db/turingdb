// Copyright 2023 Turing Biosystems Ltd.

#include "StringIndex.h"

using namespace db;

StringIndex::StringIndex()
{
}

StringIndex::~StringIndex() {
    clear();
}

StringRef StringIndex::getString(const std::string& str) {
    if (str.empty()) {
        return StringRef();
    }

    const auto it = _strMap.find(str);
    if (it != _strMap.end()) {
        return StringRef(it->second);
    }

    const std::size_t newID = _strMap.size();
    SharedString* sharedStr = new SharedString(newID, str);
    _strMap[str] = sharedStr;
    return StringRef(sharedStr);
}

bool StringIndex::hasString(const std::string& str) const {
    return _strMap.find(str) != _strMap.end();
}

void StringIndex::clear() {
    for (const auto& [rstr, sstr] : _strMap) {
        delete sstr;
    }
    _strMap.clear();
}

StringRef StringIndex::insertString(const std::string& str, size_t id) {
    SharedString* newStr = new SharedString(id, str);
    _strMap[str] = newStr;

    return StringRef(newStr);
}
