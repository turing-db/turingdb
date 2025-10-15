#include "CommitHistory.h"

#include "CommitHistoryRebaser.h"
#include "CommitView.h"

using namespace db;

CommitHistory::CommitHistory() = default;

CommitHistory::~CommitHistory() = default;

std::span<const CommitView> CommitHistory::commits() const {
    return std::span<const CommitView>(_commits);
}

void CommitHistory::newCommitHistoryFromPrevious(const CommitHistory& previous) {
    _commits = previous._commits;
    _allDataparts = previous._allDataparts;
    _commitDataparts = {};
}

