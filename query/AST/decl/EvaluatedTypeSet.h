#pragma once

#include <stddef.h>

#include <bitset>
#include <string>
#include <utility>

#include "decl/EvaluatedType.h"

namespace db {

// The types the values of an expression can have. A selection whose branches differ in
// type holds tagged cells, typed Any, yet CASE WHEN b THEN 1 ELSE 2.0 END is never a list.
// Any in the set stands for every type.
class EvaluatedTypeSet {
public:
    EvaluatedTypeSet();
    explicit EvaluatedTypeSet(EvaluatedType type);

    bool operator==(const EvaluatedTypeSet& other) const { return _types == other._types; }

    // A null branch adds no type, and a branch of any type makes the selection any type
    void addBranch(const EvaluatedTypeSet& branch);

    template <typename Predicate>
    bool holdsATypeThat(Predicate predicate) const {
        for (size_t ordinal = 0; ordinal < _types.size(); ordinal++) {
            if (_types.test(ordinal) && predicate(static_cast<EvaluatedType>(ordinal))) {
                return true;
            }
        }

        return false;
    }

    void writeName(std::string& name) const;

private:
    std::bitset<std::to_underlying(EvaluatedType::_SIZE)> _types;
};

}
