#pragma once

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

private:
    const ColumnTag _tag;
};

}
