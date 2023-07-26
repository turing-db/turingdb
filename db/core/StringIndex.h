// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <string>
#include <unordered_map>

#include "DBIndex.h"
#include "StringRef.h"

namespace db {
class StringIndexLoader;
class StringIndexDumper;

class StringIndex {
public:
    friend StringIndexLoader;
    friend StringIndexDumper;

    StringIndex();
    ~StringIndex();

    void clear();
    StringRef getString(const std::string& str);
    StringRef lookupString(const std::string& str) const;
    StringRef getString(DBIndex id) const;
    bool hasString(const std::string& str) const;
    size_t getSize() const { return _strMap.size(); }

private:
    std::unordered_map<std::string, SharedString*> _strMap;
    std::unordered_map<DBIndex, SharedString*> _strIdMap;

    StringRef insertString(const std::string& str, DBIndex id);
};

}
