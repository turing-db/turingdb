#pragma once

#include <stddef.h>

#include "labels/LabelMap.h"

namespace db {

class LabelMapComparator {
public:
    [[nodiscard]] static bool same(const LabelMap& a, const LabelMap& b) {
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

        return true;
    }
};

}
