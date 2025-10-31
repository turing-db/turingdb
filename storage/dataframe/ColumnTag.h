#pragma once

#include <stddef.h>

namespace db {

class ColumnTag {
public:
    struct Hash {
        size_t operator()(const ColumnTag& tag) const { return tag.value(); }
    };

    explicit ColumnTag(size_t value)
        : _value(value)
    {
    }

    size_t value() const { return _value; }

    bool operator==(const ColumnTag& other) const { return _value == other._value; }
    bool operator!=(const ColumnTag& other) const { return _value != other._value; }

    ColumnTag& operator++() { _value++; return *this; }
    ColumnTag operator++(int) { ColumnTag old = *this; _value++; return old; }

private:
    size_t _value {0};
};

}

