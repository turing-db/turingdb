#pragma once

#include <cstddef>

#include "LabelMap.h"

namespace db {

class LabelMapComparator {
public:
    [[nodiscard]] static bool same(const LabelMap& a, const LabelMap& b) {
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
