#include "CommitViewComparator.h"

#include <spdlog/spdlog.h>

#include "comparators/WriteSetComparator.h"
#include "versioning/CommitView.h"
#include "versioning/CommitHistory.h"

using namespace db;

bool CommitViewComparator::same(const CommitView& a, const CommitView& b) {
    // Verifiying commit journals
    const CommitJournal& journalA = a.history().journal();
    const CommitJournal& journalB = b.history().journal();

    const WriteSet<NodeID>& nodeWriteSetA = journalA.nodeWriteSet();
    const WriteSet<NodeID>& nodeWriteSetB = journalB.nodeWriteSet();

    if (!WriteSetComparator<NodeID>::same(nodeWriteSetA, nodeWriteSetB)) {
        spdlog::error("CommitJournal Node WriteSets are not the same.");
        return false;
    }

    const WriteSet<EdgeID>& edgeWriteSetA = journalA.edgeWriteSet();
    const WriteSet<EdgeID>& edgeWriteSetB = journalB.edgeWriteSet();

    if (!WriteSetComparator<EdgeID>::same(edgeWriteSetA, edgeWriteSetB)) {
        spdlog::error("CommitJournal Edge WriteSets are not the same.");
        return false;
    }
    return true;
}
