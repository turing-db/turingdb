#pragma once

#include <limits>
#include <stddef.h>
#include <stdint.h>

namespace db {

class ColumnTag {
public:
    using ValueType = uint32_t;

    ColumnTag() = default;

    struct Hash {
        size_t operator()(const ColumnTag& tag) const { return tag.getValue(); }
    };

    explicit ColumnTag(ValueType value)
        : _value(value)
    {
    }

    ColumnTag(ColumnTag&& other) noexcept = default;
    ColumnTag(const ColumnTag& other) = default;
    ColumnTag& operator=(ColumnTag&& other) noexcept = default;
    ColumnTag& operator=(const ColumnTag& other) = default;

    ValueType getValue() const { return _value; }

    bool operator==(const ColumnTag& other) const { return _value == other._value; }
    bool operator!=(const ColumnTag& other) const { return _value != other._value; }

    ColumnTag& operator++() { _value++; return *this; }
    ColumnTag operator++(int) { ColumnTag old = *this; _value++; return old; }

    bool isValid() const { return _value != std::numeric_limits<ValueType>::max(); }

private:
    ValueType _value {std::numeric_limits<ValueType>::max()};
};

}

