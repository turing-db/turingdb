#pragma once

#include <range/v3/view/zip.hpp>

#include "properties/PropertyManager.h"
#include "comparators/PropertyContainerComparator.h"
#include "comparators/PropertyIndexerComparator.h"

namespace db {

class PropertyManagerComparator {
public:
    [[nodiscard]] static bool same(const PropertyManager& a, const PropertyManager& b) {
        for (const auto& [ptA, containerA] : a) {
            auto itB = b.find(ptA);

            if (itB == b.end()) {
                return false;
            }

            const auto& [ptB, containerB] = *itB;

            if (ptA != ptB) {
                return false;
            }

            if (!PropertyContainerComparator::same(containerA.get(), containerB.get())) {
                return false;
            }
        }

        if (!PropertyIndexerComparator::same(a.indexers(), b.indexers())) {
            return false;
        }

        return true;
    }
};

}
