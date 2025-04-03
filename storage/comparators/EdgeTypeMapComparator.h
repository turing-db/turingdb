#pragma once

#include <stddef.h>

#include "types/EdgeTypeMap.h"

namespace db {

class EdgeTypeMapComparator {
public:
    [[nodiscard]] static bool same(const EdgeTypeMap& a, const EdgeTypeMap& b) {
        const size_t count = a.getCount();

        if (count != b.getCount()) {
            return false;
        }

        auto itA = a.begin();
        auto itB = b.begin();
        for (; itA != a.end() && itB != b.end(); ++itA, ++itB) {
            const auto& [idA, nameA] = *itA;
            const auto& [idB, nameB] = *itB;

            if (nameA != nameB) {
                return false;
            }

            if (idA != idB) {
                return false;
            }
        }
        for (size_t i = 0; i < count; i++) {
            const auto& nameA = a.getName(i);
            const auto& nameB = b.getName(i);

            if (nameA != nameB) {
                return false;
            }
        }

        return true;
    }
};

}
