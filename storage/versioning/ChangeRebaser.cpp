#include "ChangeRebaser.h"

#include "Change.h"
#include "CommitBuilder.h"
#include "CommitHistory.h"
#include "CommitHistoryRebaser.h"
#include "CommitWriteBuffer.h"
#include "DataPartRebaser.h"
#include "reader/GraphReader.h"
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

// If a @ref Change makes commits locally, it will create nodes according to what it
// thinks the next NodeID should be. This view is determined by the next node ID at
// the time the change branched from main. In subsequent commits on the Change, it is
// possible that there have been edges created using NodeID injection, e.g.
// @code
// CREATE (n:NEWNODE) <- Node created in this Change
// COMMIT             <- NEWNODE is written, assume it is given ID 5
// CREATE (n @ 5)-[e:NEWEDGE]-(m:NEWNODE) <- Edge created in this change, from node 5
// @endcode
// There may have been other Changes which have submitted to main in the time between
// this Change branching and submitting. Thus, when this Change comes to submit, there
// may have been a node with ID 5 created on main. If this Change commits
// without rebasing, NEWEDGE will be between the node with ID 5 that was created in
// the other change, and submitted to main, and not the "local" node with ID 5 as it
// should. Hence, we need to compare:
// 1. the nextNodeID on main when this Change was created
// 2. the nextNodeID on main when this Change attempts to submit
// If they are different, there have been nodes created on main between Change
// creation and submission. We therefore need to readjust any edges which reference a
// NodeID which is greater than the nextNodeID at time of Change creation, as these
// are edges between locally created nodes.
NodeID ChangeRebaser::rebaseNodeID(NodeID old) {
    bioassert(_branchTimeNextNodeID <= _newNextNodeID);
    return old >= _branchTimeNextNodeID ? old + _newNextNodeID - _branchTimeNextNodeID
                                        : old;
}

EdgeID ChangeRebaser::rebaseEdgeID(EdgeID old) {
    bioassert(_branchTimeNextEdgeID <= _newNextEdgeID);
    return old >= _branchTimeNextEdgeID ? old + _newNextEdgeID - _branchTimeNextEdgeID
                                        : old;
}

void ChangeRebaser::init(const GraphReader& mainReader,
                               const GraphReader& branchTimeReader) {
    bioassert(_branchTimeReader == nullptr);
    bioassert(_newMainReader == nullptr);

    _branchTimeReader = &branchTimeReader;
    _newMainReader = &mainReader;

    _branchTimeNextNodeID = branchTimeReader.getNodeCount();
    _branchTimeNextEdgeID = branchTimeReader.getEdgeCount();
    _newNextNodeID = mainReader.getNodeCount();
    _newNextEdgeID = mainReader.getEdgeCount();

    // _metadataRebaser already intialised: requires no args
    _dataPartRebaser = DataPartRebaser(this);
}

void ChangeRebaser::rebaseCommitBuilder(CommitBuilder& commitBuilder) {
    CommitData& data = commitBuilder.commitData();
    CommitHistory& history = data.history();

    CommitHistoryRebaser historyRebaser {history};

    // Undo any commits that were made locally
    // Only check those with an non-empty writebuffer, as other sources e.g.
    // GraphWriter will create multiple dataparts from builders at commit-time but not
    // at submit time, and so should not be erased
    if (!commitBuilder.writeBuffer().isFlushed()) {
        historyRebaser.undoLocalCommits();
        // We have deleted all created DPs: reset this number
        commitBuilder._datapartCount = 0;
        commitBuilder.writeBuffer().setUnflushed();
    }

    CommitWriteBufferRebaser wbRb(this, commitBuilder.writeBuffer());
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
