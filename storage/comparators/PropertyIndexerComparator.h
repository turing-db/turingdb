#pragma once

#include <range/v3/view/zip.hpp>

#include "PropertyIndexer.h"

namespace db {

class PropertyIndexerComparator {
public:
    [[nodiscard]] static bool same(const PropertyIndexer& a,
                                   const PropertyIndexer& b) {
        namespace rv = ranges::views;

        if (a.size() != b.size()) {
            return false;
        }

        for (const auto& [ptID, indexerA] : a) {
            const auto itB = b.find(ptID);

            if (itB == b.end()) {
                return false;
            }

            const auto& indexerB = itB->second;

            for (const auto& [lsetID, infoA] : indexerA) {
                const auto infoItB = indexerB.find(lsetID);

                if (infoItB == indexerB.end()) {
                    return false;
                }

                const auto& infoB = infoItB->second;

                if (infoA._binarySearched != infoB._binarySearched) {
                    return false;
                }

                for (const auto& [rA, rB] : rv::zip(infoA._ranges, infoB._ranges)) {
                    if (rA._firstID != rB._firstID) {
                        return false;
                    }

                    if (rA._firstPos != rB._firstPos) {
                        return false;
                    }

                    if (rA._count != rB._count) {
                        return false;
                    }
                }
            }
        }

        return true;
    }
};

}
