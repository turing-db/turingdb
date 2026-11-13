#include "EvaluatedTypeSet.h"

#include <string_view>

using namespace db;

EvaluatedTypeSet::EvaluatedTypeSet() {
}

EvaluatedTypeSet::EvaluatedTypeSet(EvaluatedType type) {
    _types.set(std::to_underlying(type));
}

void EvaluatedTypeSet::addBranch(const EvaluatedTypeSet& branch) {
    const size_t anyOrdinal = std::to_underlying(EvaluatedType::Any);

    _types |= branch._types;
    _types.reset(std::to_underlying(EvaluatedType::Null));

    if (_types.test(anyOrdinal)) {
        _types.reset();
        _types.set(anyOrdinal);
    }
}

void EvaluatedTypeSet::writeName(std::string& name) const {
    std::string_view separator;

    for (size_t ordinal = 0; ordinal < _types.size(); ordinal++) {
        if (!_types.test(ordinal)) {
            continue;
        }

        name += separator;
        name += EvaluatedTypeName::value(static_cast<EvaluatedType>(ordinal));
        separator = " or ";
    }
}
