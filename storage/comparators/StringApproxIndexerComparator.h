#pragma once

#include "indexers/StringPropertyIndexer.h"

namespace db {
class StringApproxIndexerComparator {
public:
    [[nodiscard]] static bool same(const StringPropertyIndexer& a,
                                   const StringPropertyIndexer& b);

private:
    [[nodiscard]] static bool index_same(const StringIndex& a, const StringIndex& b);
};

}
