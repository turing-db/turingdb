#include "ChangeRebaser.h"

#include "Change.h"
#include "Commit.h"
#include "CommitBuilder.h"
#include "CommitHistory.h"
#include "CommitHistoryRebaser.h"
#include "EntityIDRebaser.h"
#include "CommitWriteBuffer.h"
#include "DataPartRebaser.h"
#include "reader/GraphReader.h"
#include "ChangeConflictChecker.h"
#include "Tombstones.h"

#include "BioAssert.h"

using namespace db;

ChangeRebaser::ChangeRebaser(Change& change,
                             const CommitData* currentHeadCommitData,
                             const CommitHistory* currentHeadHistory)
    : _change {&change},
    _currentHeadCommitData(currentHeadCommitData),
    _currentHeadHistory(currentHeadHistory)
{
}

void ChangeRebaser::init(const GraphReader& mainReader,
                         const GraphReader& branchTimeReader) {
    _branchTimeReader = &branchTimeReader;
    _newMainReader = &mainReader;

    NodeID branchTimeNextNodeID = branchTimeReader.getTotalNodesAllocated();
    EdgeID branchTimeNextEdgeID = branchTimeReader.getTotalEdgesAllocated();
    _newNextNodeID = mainReader.getTotalNodesAllocated();
    _newNextEdgeID = mainReader.getTotalEdgesAllocated();

    _entityIDRebaser = EntityIDRebaser(branchTimeNextNodeID,
                                       branchTimeNextEdgeID,
                                       _newNextNodeID,
                                       _newNextEdgeID);

    // _metadataRebaser already intialised: requires no args
    _dataPartRebaser = DataPartRebaser(&_entityIDRebaser);
}

void ChangeRebaser::checkConflicts(const Commit::CommitSpan commits) {
    ChangeConflictChecker conflictChecker(*_change, _entityIDRebaser, commits);
    conflictChecker.checkConflicts();
}

void ChangeRebaser::rebaseTombstones(Tombstones& tombstones) {
    bioassert(!_newMainReader->commits().empty(),
              "Attempted to rebase tombstones without any commits on main.");

    Tombstones::NodeTombstones& nodeTombstones = tombstones._nodeTombstones;
    Tombstones::EdgeTombstones& edgeTombstones = tombstones._edgeTombstones;

    // Reassign the IDs to be in sync with main
    if (!nodeTombstones.empty()) {
        Tombstones::NodeTombstones temp;
        for (const NodeID node : nodeTombstones) {
            temp.insert(_entityIDRebaser.rebaseNodeID(node));
        }
        nodeTombstones.swap(temp);
    }
    if (!edgeTombstones.empty()) {
        Tombstones::EdgeTombstones temp;
        for (const EdgeID edge : edgeTombstones) {
            temp.insert(_entityIDRebaser.rebaseEdgeID(edge));
        }
        edgeTombstones.swap(temp);
    }

    // We want all the tombstones on main unioned with the rebased tombstones we created
    const Tombstones& mainTombstones = _newMainReader->commits().back().tombstones();

    const Tombstones::NodeTombstones& mainNodeTombstones =
        mainTombstones.nodeTombstones();
    const Tombstones::EdgeTombstones& mainEdgeTombstones =
        mainTombstones.edgeTombstones();

    // Perform set union on main and rebased tombstones
    Tombstones::NodeTombstones::setUnion(nodeTombstones, mainNodeTombstones);
    Tombstones::EdgeTombstones::setUnion(edgeTombstones, mainEdgeTombstones);
}

void ChangeRebaser::rebaseCommitBuilder(CommitBuilder& commitBuilder) {
    CommitData& data = commitBuilder.commitData();
    CommitHistory& history = data.history();

    // Even if this commit hasn't created tombstones, we need to union them with the
    // tombstones on main, so always rebase
    Tombstones& commitTombstones = commitBuilder._commitData->_tombstones;
    rebaseTombstones(commitTombstones);

    CommitHistoryRebaser historyRebaser {history};

    // If we have not yet flushed, we must rebase the write buffer prior to it being flushed
    if (!commitBuilder.writeBuffer().isFlushed()) {
        CommitWriteBufferRebaser wbRb(&_entityIDRebaser, commitBuilder.writeBuffer());
        wbRb.rebase();
    }

    _metadataRebaser.clear();
    _metadataRebaser.rebase(_currentHeadCommitData->metadata(),
                            commitBuilder.metadata(),
                            commitBuilder.writeBuffer());

    historyRebaser.rebase(_metadataRebaser, _dataPartRebaser, *_currentHeadHistory);

    _currentHeadCommitData = &commitBuilder.commitData();
    _currentHeadHistory = &_currentHeadCommitData->history();
}
