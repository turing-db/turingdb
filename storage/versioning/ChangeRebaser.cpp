#include "ChangeRebaser.h"

#include "Change.h"
#include "CommitBuilder.h"
#include "CommitHistory.h"
#include "CommitHistoryRebaser.h"
#include "EntityIDRebaser.h"
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
