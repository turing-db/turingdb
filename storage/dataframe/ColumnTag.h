#pragma once

#include <stddef.h>

namespace db {

class ColumnTag {
public:
    struct Hash {
        size_t operator()(const ColumnTag& tag) const { return tag.getValue(); }
    };

    ColumnTag() = default;
    ~ColumnTag() = default;

    explicit ColumnTag(size_t value)
        : _value(value)
    {
    }

    ColumnTag(ColumnTag&& other) noexcept = default;
    ColumnTag(const ColumnTag& other) = default;
    ColumnTag& operator=(ColumnTag&& other) noexcept = default;
    ColumnTag& operator=(const ColumnTag& other) = default;

    size_t getValue() const { return _value; }

    bool operator==(const ColumnTag& other) const { return _value == other._value; }
    bool operator!=(const ColumnTag& other) const { return _value != other._value; }

    ColumnTag& operator++() { _value++; return *this; }
    ColumnTag operator++(int) { ColumnTag old = *this; _value++; return old; }

private:
    size_t _value {0};
};

}

