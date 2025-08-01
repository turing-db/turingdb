#include "StringApproxIndexerComparator.h"

using namespace db;

bool StringApproxIndexerComparator::index_same(const StringIndex& a,
                                               const StringIndex& b) {
    auto ita = a.begin();
    auto itb = b.begin();

    // For each entry
    for (; ita != a.end() || itb != b.end(); ++ita, ++itb) {
        // The word must be the same
        if (ita->word != itb->word) {
            return false;
        }

        auto oa = ita->owners;
        auto ob = itb->owners;

        auto itoa = oa.begin();
        auto itob = ob.begin();

        // They must have the same owners
        for (; itoa != oa.end() || itob != ob.end(); ++itoa, ++itob) {
            if (itoa->getValue() != itob->getValue()) {
                return false;
            }
        }
    }

    return true;
}

bool StringApproxIndexerComparator::same(const StringPropertyIndexer& a,
                                         const StringPropertyIndexer& b) {
    // Same number of properties indexed
    if (a.size() != b.size()) {
        return false;
    }

    auto ita = a.begin();
    auto itb = b.begin();

    for (; ita != a.end() || itb != b.end(); ita++, itb++) {
        // Same property IDs
        if (ita->first != itb->first) {
            return false;
        }

        // Same strings-owners stored
        if (!index_same(*ita->second, *itb->second)) {
            return false;
        }
    }

    return true;
}
