#include "CommitHistory.h"

#include "CommitHistoryRebaser.h"
#include "CommitView.h"
#include "Panic.h"

using namespace db;

CommitHistory::CommitHistory() = default;

CommitHistory::~CommitHistory() = default;

std::span<const CommitView> CommitHistory::commits() const {
    return std::span<const CommitView>(_commits);
}

std::span<const CommitView> CommitHistory::getAllSince(CommitHash hash) const {
    ssize_t startIndex = -1;

    for (size_t i = 0; i < _commits.size(); ++i) {
        if (_commits[i].hash() == hash) {
            startIndex = i;
            break;
        }
    }

    if (startIndex == -1) {
        panic("Could not find Commit with hash {:x}", hash.get());
    }

    startIndex++;
    const size_t count = _commits.size() - startIndex;

    return {_commits.data() + startIndex, count};
}

void CommitHistory::newCommitHistoryFromPrevious(const CommitHistory& previous) {
    _commits = previous._commits;
    _allDataparts = previous._allDataparts;
    _commitDataparts = {};
}

