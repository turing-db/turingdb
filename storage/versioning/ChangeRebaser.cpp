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
#include "BioAssert.h"
#include "ChangeConflictChecker.h"
#include "Tombstones.h"

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
    bioassert(_branchTimeReader == nullptr);
    bioassert(_newMainReader == nullptr);

    _branchTimeReader = &branchTimeReader;
    _newMainReader = &mainReader;

    NodeID branchTimeNextNodeID = branchTimeReader.getNodeCount();
    EdgeID branchTimeNextEdgeID = branchTimeReader.getEdgeCount();
    _newNextNodeID = mainReader.getNodeCount();
    _newNextEdgeID = mainReader.getEdgeCount();

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
    bioassert(!_newMainReader->commits().empty());

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

    // If we made a local commit (signified by the write buffer being flushed), undo it
    if (!commitBuilder.writeBuffer().isFlushed()) {
        historyRebaser.removeCreatedDataParts();
        // We have deleted all created DPs: reset this number
        commitBuilder._datapartCount = 0;
        commitBuilder.writeBuffer().setUnflushed();
    }

    CommitWriteBufferRebaser wbRb(&_entityIDRebaser, commitBuilder.writeBuffer());
    wbRb.rebase();

    // These values are initially set at time of the creation of this Change, however
    // they need to be updated to point to the next ID on the current state of main.
    // These values will be used when creating new dataparts at time of submit.
    commitBuilder._nextNodeID = commitBuilder._firstNodeID = _newNextNodeID;
    commitBuilder._nextEdgeID = commitBuilder._firstEdgeID = _newNextEdgeID;

    _metadataRebaser.clear();
    _metadataRebaser.rebase(_currentHeadCommitData->metadata(), commitBuilder.metadata(),
                            commitBuilder.writeBuffer());

    historyRebaser.rebase(_metadataRebaser, _dataPartRebaser, *_currentHeadHistory);

    _currentHeadCommitData = &commitBuilder.commitData();
    _currentHeadHistory = &_currentHeadCommitData->history();
}
