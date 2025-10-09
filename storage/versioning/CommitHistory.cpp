#include "CommitHistory.h"

#include "BioAssert.h"
#include "DataPartRebaser.h"
#include "CommitView.h"
#include "CommitData.h"

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

void CommitHistoryRebaser::rebase(const MetadataRebaser& metadataRebaser,
                                  DataPartRebaser& dataPartRebaser,
                                  const CommitHistory& prevHistory) {
    // Commits
    const CommitView tip = _history._commits.back();
    _history._commits = prevHistory._commits;
    _history._commits.push_back(tip);

    // Dataparts
    auto newDataparts = prevHistory._allDataparts;

    const size_t commitDatapartCount = _history._commitDataparts.size();
    newDataparts.resize(newDataparts.size() + commitDatapartCount);
    std::copy(_history._commitDataparts.begin(),
              _history._commitDataparts.end(),
              newDataparts.begin() + prevHistory._allDataparts.size());

    _history._allDataparts = std::move(newDataparts);
    _history._commitDataparts = {
        _history._allDataparts.data() + _history._allDataparts.size() - commitDatapartCount,
        commitDatapartCount,
    };

    const auto& prevDataParts = prevHistory._allDataparts;

    if (!prevDataParts.empty()) {
        const auto* prevPart = prevDataParts.back().get();
        for (auto& part : _history._commitDataparts) {
            dataPartRebaser.rebase(metadataRebaser, *prevPart, *part);
            prevPart = part.get();
        }
    }
}

void CommitHistoryBuilder::undoLocalCreates() {
    // Total number of dataparts in the view of this commit
    const size_t totalDPs = _history._allDataparts.size();
    // Total number of datapart which were created as part of this commit, as a result
    // of Change::commit (1 commit = 1 datapart).
    const size_t committedDPs = _history._commitDataparts.size();
    // Just delete the most recent committedDPs number of DPs
    resizeDataParts(totalDPs - committedDPs);
    // Reset this commit to have no locally created DPs
    setCommitDatapartCount(0);
}

void CommitHistoryBuilder::undoLocalDeletes(const CommitData& base) {
    const std::span unmodifiedDataParts = base.allDataparts(); // Original base
    std::span modifiedDataParts = _history.allDataPartsMutVec(); // Modified versions
    bioassert(modifiedDataParts.size() >= unmodifiedDataParts.size());

    std::copy(unmodifiedDataParts.begin(), unmodifiedDataParts.end(), modifiedDataParts.begin());
}
