#pragma once

#include <range/v3/view/zip.hpp>

#include "datapart/DegreeProducts.h"

namespace db {

class DegreeProductsComparator {
public:
    [[nodiscard]] static bool same(const DegreeProducts& a, const DegreeProducts& b) {
        namespace rv = ranges::views;

        const std::span<const DegreeProduct> productsA = a.getProducts();
        const std::span<const DegreeProduct> productsB = b.getProducts();

        if (productsA.size() != productsB.size()) {
            return false;
        }

        for (const auto& [productA, productB] : rv::zip(productsA, productsB)) {
            const bool sameFirst = productA._first._edgeTypeID == productB._first._edgeTypeID
                                && productA._first._isOut == productB._first._isOut;
            const bool sameSecond = productA._second._edgeTypeID == productB._second._edgeTypeID
                                 && productA._second._isOut == productB._second._isOut;
            const bool sameLabelSet = productA._labelSet.getID() == productB._labelSet.getID();

            if (!sameLabelSet || !sameFirst || !sameSecond || productA._sum != productB._sum) {
                return false;
            }
        }

        return true;
    }
};

}
