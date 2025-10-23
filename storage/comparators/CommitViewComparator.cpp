#include "CommitViewComparator.h"

#include <spdlog/spdlog.h>

#include "comparators/WriteSetComparator.h"
#include "comparators/TombstoneSetComparator.h"
#include "versioning/CommitData.h"
#include "versioning/CommitView.h"
#include "versioning/CommitHistory.h"

using namespace db;

bool CommitViewComparator::same(const CommitView& a, const CommitView& b) {
    // Verifiying commit journals
    {
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
    }

    // Verifiying tombstones
    {
        const Tombstones& tombstonesA = a.data().tombstones();
        const Tombstones& tombstonesB = b.data().tombstones();

        const Tombstones::NodeTombstones& nodesA = tombstonesA.nodeTombstones();
        const Tombstones::NodeTombstones& nodesB = tombstonesB.nodeTombstones();

        if (!TombstoneSetComparator<NodeID>::same(nodesA, nodesB)) {
            spdlog::error("Node TombstoneSets are not the same.");
            return false;
        }

        const Tombstones::EdgeTombstones& edgesA = tombstonesA.edgeTombstones();
        const Tombstones::EdgeTombstones& edgesB = tombstonesB.edgeTombstones();

        if (!TombstoneSetComparator<EdgeID>::same(edgesA, edgesB)) {
            spdlog::error("Edge TombstoneSets are not the same.");
            return false;
        }
    }

    return true;
}
