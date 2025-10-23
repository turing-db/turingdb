#include "TombstoneSetComparator.h"

#include <spdlog/spdlog.h>

#include "versioning/TombstoneSet.h"

using namespace db;

namespace db {
template class TombstoneSetComparator<NodeID>;
template class TombstoneSetComparator<EdgeID>;
}

template <TypedInternalID IDT>
bool db::TombstoneSetComparator<IDT>::same(const TombstoneSet<IDT>& setA, const TombstoneSet<IDT>& setB) {
    constexpr std::string_view type = std::is_same_v<IDT, NodeID> ? "Node" : "Edge";

    if (setA.size() != setB.size()) {
        spdlog::error("{} TombstoneSets have sizes {} and {}, respectively.", type,
                      setA.size(), setB.size());
        return false;
    }

    for (IDT entry : setA) {
        if (!setB.contains(entry)) {
            spdlog::error(
                "{} TombStoneSet A contains {}, but {} TombstoneSet B does not.", type,
                entry.getValue(), type);
            return false;
        }
    }

    return true;
}
