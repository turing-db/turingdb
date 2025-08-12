#include "StringIndexerComparator.h"
#include "spdlog/spdlog.h"

using namespace db;

bool StringIndexerComparator::index_same(const StringIndex& a,
                                               const StringIndex& b) {
    auto ita = a.begin();
    auto itb = b.begin();

    // For each entry
    for (; ita != a.end() || itb != b.end(); ++ita, ++itb) {
        // The word must be the same
        if (ita->word != itb->word) {
            spdlog::error("A has {} whilst B has {}", ita->word, itb->word);
            return false;
        }

        auto oa = ita->owners;
        auto ob = itb->owners;
        if (oa.size() != ob.size()) {
            spdlog::info("A has {} owners for word {} whilst B has {}", oa.size(),
                         ita->word, ob.size());
            return false;
        }

        auto itoa = oa.begin();
        auto itob = ob.begin();

        // They must have the same owners
        for (; itoa != oa.end() || itob != ob.end(); ++itoa, ++itob) {
            if (itoa->getValue() != itob->getValue()) {
                spdlog::error("A has owner {}  for {} whilst B has ", itoa->getValue(),ita->word, itob->getValue());
                return false;
            }
        }
    }

    return true;
}

bool StringIndexerComparator::same(const StringPropertyIndexer& a,
                                         const StringPropertyIndexer& b) {
    // Same number of properties indexed
    if (a.size() != b.size()) {
        spdlog::error("Indexers are not same size");
        return false;
    }

    auto ita = a.begin();

    for (; ita != a.end(); ita++) {
        auto itb = b.find(ita->first);
        if (itb == b.end()) {
            spdlog::error("A has index for propID {} whilst B does not", ita->first);
            return false;
        }

        // Same strings-owners stored
        if (!index_same(*ita->second, *itb->second)) {
            return false;
        }
    }

    return true;
}
