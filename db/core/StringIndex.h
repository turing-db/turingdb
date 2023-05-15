// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <string>
#include <unordered_map>

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
    bool hasString(const std::string& str) const;
    size_t getSize() const { return _strMap.size(); }

private:
    std::unordered_map<std::string, SharedString*> _strMap;

    void insertString(const std::string& str, std::size_t id);
};

}
