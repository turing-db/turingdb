#include "WriteSetComparator.h"

#include <spdlog/spdlog.h>

#include "versioning/WriteSet.h"

namespace db {

template <TypedInternalID IDT>
bool WriteSetComparator<IDT>::same(const WriteSet<IDT>& setA , const WriteSet<IDT>& setB) {
    constexpr std::string_view type = std::is_same_v<IDT, NodeID> ? "Node" : "Edge";

    if (setA.size() != setB.size()) {
        spdlog::error("{} WriteSets in CommitJournal have sizes {} and {}, respectively.",
                      type, setA.size(), setB.size());
        return false;
    }

    auto itA = setA.begin();
    auto itB = setB.begin();
    size_t index = 0;
    while (itA != setA.end() && itB != setB.end()) {
        if (*itA != *itB) {
            spdlog::error("{} WriteSets have values {} and {}, respectively, at index {}",
                          *itA, type, *itB, index);
            return false;
        }
        itA++;
        itB++;
        index++;
    }

    return true;
}

// Explicit template instantiations
template class WriteSetComparator<NodeID>;
template class WriteSetComparator<EdgeID>;

} // namespace db
