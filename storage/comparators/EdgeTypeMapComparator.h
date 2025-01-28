#pragma once

#include <cstddef>

#include "EdgeTypeMap.h"

namespace db {

class EdgeTypeMapComparator {
public:
    [[nodiscard]] static bool same(const EdgeTypeMap& a, const EdgeTypeMap& b) {
        const size_t count = a.getCount();

        if (count != b.getCount()) {
            return false;
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
