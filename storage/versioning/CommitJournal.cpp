#include "CommitJournal.h"

#include "versioning/CommitWriteBuffer.h"

using namespace db;


CommitJournal* CommitJournal::newJournal(const CommitWriteBuffer& wb) {
    const std::set<NodeID>& deletedNodes = wb.deletedNodes();
    const std::set<EdgeID>& deletedEdges = wb.deletedEdges();

    CommitJournal* journal = new CommitJournal;

    // 1. Generate _deletedNodeRanges
    const auto contiguousIDs = [](NodeID a, NodeID b) { return !(b == a + 1); };

    auto rangeStartIt = deletedNodes.begin();
    auto rangeEndIt = std::adjacent_find(deletedNodes.begin(), deletedNodes.end(), contiguousIDs);

    while (rangeEndIt != deletedNodes.end()) {
        size_t count = (*rangeEndIt - *rangeStartIt).getValue();

        journal->_deletedNodeRanges.emplace_back(*rangeStartIt, count);

        rangeStartIt = rangeEndIt;

        rangeEndIt = std::adjacent_find(rangeStartIt, deletedNodes.end(), contiguousIDs);
    }





    // 2. Generate _deletedEdges
    // From set, so already sorted in EdgeID asc.
    std::vector<EdgeID> journalDelEdges(deletedEdges.begin(), deletedEdges.end());
    journal->_deletedEdges = std::move(journalDelEdges);

    return {};
}
