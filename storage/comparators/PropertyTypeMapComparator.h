#pragma once

#include <stddef.h>

#include "types/PropertyTypeMap.h"

namespace db {

class PropertyTypeMapComparator {
public:
    [[nodiscard]] static bool same(const PropertyTypeMap& a, const PropertyTypeMap& b) {
        const size_t count = a.getCount();

        if (count != b.getCount()) {
            return false;
        }

        auto itA = a.begin();
        auto itB = b.begin();
        for (; itA != a.end() && itB != b.end(); ++itA, ++itB) {
            const auto& [ptA, nameA] = *itA;
            const auto& [ptB, nameB] = *itB;

            if (nameA != nameB) {
                return false;
            }

            if (ptA._valueType != ptB._valueType) {
                return false;
            }

            if (ptA._id != ptB._id) {
                return false;
            }
        }

        return true;
    }
};

}
