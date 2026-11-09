#include "EvaluatedTypeSet.h"

#include <string_view>

using namespace db;

namespace {

uint32_t bitOf(EvaluatedType type) {
    return static_cast<uint32_t>(1) << static_cast<uint32_t>(type);
}

}

static_assert(std::to_underlying(EvaluatedType::_SIZE) <= 32, "EvaluatedTypeSet holds 32 types");

EvaluatedTypeSet::EvaluatedTypeSet() {
}

EvaluatedTypeSet::EvaluatedTypeSet(EvaluatedType type)
    : _bitset(bitOf(type))
{
}

bool EvaluatedTypeSet::contains(EvaluatedType type) const {
    return (_bitset & bitOf(type)) != 0;
}

void EvaluatedTypeSet::addSelected(const EvaluatedTypeSet& branch) {
    _bitset |= branch._bitset & ~bitOf(EvaluatedType::Null);

    if (contains(EvaluatedType::Any)) {
        _bitset = bitOf(EvaluatedType::Any);
    }
}

void EvaluatedTypeSet::writeName(std::string& name) const {
    std::string_view separator;

    for (uint8_t ordinal = 0; ordinal < std::to_underlying(EvaluatedType::_SIZE); ordinal++) {
        const EvaluatedType type = static_cast<EvaluatedType>(ordinal);

        if (contains(type)) {
            name += separator;
            name += EvaluatedTypeName::value(type);
            separator = " or ";
        }
    }
}
