#pragma once

#include <cstddef>

#include "PropertyTypeMap.h"

namespace db {

class PropertyTypeMapComparator {
public:
    [[nodiscard]] static bool same(const PropertyTypeMap& a, const PropertyTypeMap& b) {
        const size_t count = a.getCount();

        if (count != b.getCount()) {
            return false;
        }

        for (size_t i = 0; i < count; i++) {
            const PropertyType ptA = a.get(i);
            const PropertyType ptB = a.get(i);
            const auto& nameA = a.getName(i);
            const auto& nameB = b.getName(i);

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
