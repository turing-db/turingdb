// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <string>
#include <unordered_map>

#include "StringRef.h"

namespace db {
class StringIndexSerializer;

class StringIndex {
public:
    friend StringIndexSerializer;

    StringIndex();
    ~StringIndex();

    void clear();
    StringRef getString(const std::string& str);

private:
    std::unordered_map<std::string, SharedString*> _strMap;

    void insertString(const std::string& str, std::size_t id);
};

}
