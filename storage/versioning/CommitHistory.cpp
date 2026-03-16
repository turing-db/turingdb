#include "CommitHistory.h"

#include "CommitHistoryRebaser.h"
#include "CommitView.h"

using namespace db;

CommitHistory::CommitHistory() = default;

CommitHistory::~CommitHistory() = default;


void CommitHistory::newCommitHistoryFromPrevious(const CommitHistory& previous) {
    _allDataparts = previous._allDataparts;
    _commitDataparts = {};
}

void CommitHistory::newMergeCommitHistory() {
    _allDataparts = {};
    _commitDataparts = {};
}
