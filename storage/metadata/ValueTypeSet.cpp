#include "ValueTypeSet.h"

#include <bit>

#include "BioAssert.h"

using namespace db;

ValueTypeSet ValueTypeSet::of(std::initializer_list<ValueType> types) {
    uint32_t bits = 0;
    for (const ValueType type : types) {
        bits |= bit(type);
    }

    return ValueTypeSet(bits);
}

ValueType ValueTypeSet::getSingle() const {
    bioassert(isSingle(), "A single value type is read off a set holding exactly one");

    return static_cast<ValueType>(std::countr_zero(_bits));
}
