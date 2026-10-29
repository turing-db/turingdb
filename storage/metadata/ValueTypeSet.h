#pragma once

#include <stdint.h>

#include <initializer_list>

#include "metadata/PropertyType.h"

namespace db {

// The value types a query parameter's bind accepts: one type, a subset, or any of them
class ValueTypeSet {
public:
    constexpr ValueTypeSet() = default;

    static constexpr ValueTypeSet any() { return ValueTypeSet(ALL); }
    static constexpr ValueTypeSet of(ValueType type) { return ValueTypeSet(bit(type)); }
    static ValueTypeSet of(std::initializer_list<ValueType> types);
    static constexpr ValueTypeSet fromBits(uint32_t bits) { return ValueTypeSet(bits); }

    constexpr bool contains(ValueType type) const { return (_bits & bit(type)) != 0; }
    constexpr bool isAny() const { return _bits == ALL; }
    constexpr bool isSingle() const { return _bits != 0 && (_bits & (_bits - 1)) == 0; }
    ValueType getSingle() const;

    constexpr bool operator==(const ValueTypeSet& other) const = default;

    constexpr uint32_t getBits() const { return _bits; }

private:
    static constexpr uint32_t ALL = ((1U << static_cast<uint32_t>(ValueType::_SIZE)) - 1) & ~1U;
    static_assert(static_cast<size_t>(ValueType::_SIZE) <= 32);

    uint32_t _bits {0};

    constexpr explicit ValueTypeSet(uint32_t bits)
        : _bits(bits)
    {
    }

    static constexpr uint32_t bit(ValueType type) { return 1U << static_cast<uint32_t>(type); }
};

}
