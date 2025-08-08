#pragma once

#include "indexers/StringPropertyIndexer.h"

namespace db {

class StringIndexerComparator {
public:
    [[nodiscard]] static bool same(const StringPropertyIndexer& a,
                                   const StringPropertyIndexer& b);

private:
    [[nodiscard]] static bool indexSame(const StringIndex& a, const StringIndex& b);
    [[nodiscard]] static bool nodeSame(StringIndex::PrefixTreeNode* a,
                                       StringIndex::PrefixTreeNode* b);
};

}
