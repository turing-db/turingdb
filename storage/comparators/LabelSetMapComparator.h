#pragma once

#include <cstddef>

#include "LabelSetMap.h"

namespace db {

class LabelSetMapComparator {
public:
    [[nodiscard]] static bool same(const LabelSetMap& a, const LabelSetMap& b) {
        const size_t count = a.getCount();

        if (count != b.getCount()) {
            return false;
        }

        for (size_t i = 0; i < count; i++) {
            const auto& setA = a.getValue(i);
            const auto& setB = b.getValue(i);

            if (setA != setB) {
                return false;
            }
        }

        return true;
    }
};

}
