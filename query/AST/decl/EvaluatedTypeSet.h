#pragma once

#include <stdint.h>

#include <string>
#include <utility>

#include "decl/EvaluatedType.h"

namespace db {

// The types the values of an expression can have. A rule taking an Any for the row's own
// type takes one built by a selection only where it takes one of the selected types:
// CASE WHEN b THEN 1 ELSE 2.0 END is never a list.
class EvaluatedTypeSet {
public:
    EvaluatedTypeSet();
    explicit EvaluatedTypeSet(EvaluatedType type);

    bool operator==(const EvaluatedTypeSet& other) const { return _bitset == other._bitset; }

    bool contains(EvaluatedType type) const;

    // Adds the types the values of one branch of a selection have. A null branch adds none,
    // and the row's own type stands for every other.
    void addSelected(const EvaluatedTypeSet& branch);

    template <typename Predicate>
    bool holdsATypeThat(Predicate predicate) const {
        for (uint8_t ordinal = 0; ordinal < std::to_underlying(EvaluatedType::_SIZE); ordinal++) {
            const EvaluatedType type = static_cast<EvaluatedType>(ordinal);

            if (contains(type) && predicate(type)) {
                return true;
            }
        }

        return false;
    }

    void writeName(std::string& name) const;

private:
    uint32_t _bitset {0};
};

}
