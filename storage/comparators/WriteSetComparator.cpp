#include "WriteSetComparator.h"
#include "ID.h"
#include "spdlog/spdlog.h"

using namespace db;

namespace db {
    template class WriteSetComparator<NodeID>;
    template class WriteSetComparator<EdgeID>;
}

template <TypedInternalID IDT>
bool WriteSetComparator<IDT>::same(const WriteSet<IDT> setA , const WriteSet<IDT> setB) {
    if (setA.size() != setB.size()) {
        spdlog::error(
            "Node WriteSets in CommitJournal have sizes {} and {}, respecitvely.",
            setA.size(), setB.size());
        return false;
    }

    auto itA = setA.begin();
    auto itB = setB.begin();
    size_t index = 0;
    while (itA != setA.end() && itB != setB.end()) {
        if (*itA != *itB) {
            spdlog::error(
                "Node WriteSets have values {} and {}, respecitvely, at index {}", *itA,
                *itB, index);
            return false;
        }
        itA++;
        itB++;
        index++;
    }

    return true;
}
