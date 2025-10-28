#pragma once

#include <string_view>

#include "ColumnTag.h"

namespace db {

class ColumnHeader {
public:
    explicit ColumnHeader(ColumnTag tag)
        : _tag(tag)
    {
    }

    ~ColumnHeader() = default;

    bool operator==(const ColumnHeader& other) const {
        return _tag == other._tag;
    }

    ColumnTag getTag() const { return _tag; }

    bool hasName() const { return _name.empty(); }

    std::string_view getName() const { return _name; }

    void setName(std::string_view name) { _name = name; }

private:
    const ColumnTag _tag;
    std::string_view _name;
};

}
