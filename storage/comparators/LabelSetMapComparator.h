#pragma once

#include <stddef.h>

#include "labels/LabelSetMap.h"

namespace db {

class LabelSetMapComparator {
public:
    [[nodiscard]] static bool same(const LabelSetMap& a, const LabelSetMap& b) {
        const size_t count = a.getCount();

        if (count != b.getCount()) {
            return false;
        }

        auto itA = a.begin();
        auto itB = b.begin();

        for (; itA != a.end() && itB != b.end(); ++itA, ++itB) {
            const auto& [idA, setA] = *itA;
            const auto& [idB, setB] = *itB;

            if (idA != idB) {
                return false;
            }

            if (setA != setB) {
                return false;
            }
        }

        return true;
    }
};

}
