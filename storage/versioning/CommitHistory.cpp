#include "CommitHistory.h"

#include "CommitHistoryRebaser.h"
#include "CommitView.h"

using namespace db;

CommitHistory::CommitHistory() = default;

CommitHistory::~CommitHistory() = default;

std::span<const CommitView> CommitHistory::commits() const {
    return std::span<const CommitView>(_commits);
}

void CommitHistory::newFromPrevious(const CommitHistory& base) {
    _commits = base._commits;
    _allDataparts = base._allDataparts;
    _commitDataparts = {};
}

