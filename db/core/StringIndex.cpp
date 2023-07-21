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

    const DBIndex newID{_strMap.size()};
    SharedString* sharedStr = new SharedString(newID, str);
    _strMap[str] = sharedStr;
    _strIdMap[newID] = sharedStr;

    return StringRef(sharedStr);
}

StringRef StringIndex::lookupString(const std::string& str) const {
    return _strMap.at(str);
}

StringRef StringIndex::getString(DBIndex id) const {
    const auto it = _strIdMap.find(id);
    if (it == _strIdMap.end()) {
        return StringRef();
    }

    return StringRef(it->second);
}

bool StringIndex::hasString(const std::string& str) const {
    return _strMap.find(str) != _strMap.end();
}

void StringIndex::clear() {
    for (const auto& [rstr, sstr] : _strMap) {
        delete sstr;
    }

    _strMap.clear();
    _strIdMap.clear();
}

StringRef StringIndex::insertString(const std::string& str, DBIndex id) {
    SharedString* newStr = new SharedString(id, str);
    _strMap[str] = newStr;
    _strIdMap[id] = newStr;

    return StringRef(newStr);
}
