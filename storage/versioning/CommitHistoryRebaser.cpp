#include "CommitHistoryRebaser.h"

#include <spdlog/spdlog.h>

#include "CommitView.h"
#include "CommitHistory.h"
#include "DataPartRebaser.h"

#include "versioning/EntityIDRebaser.h"
#include "versioning/MetadataRebaser.h"

using namespace db;

void CommitHistoryRebaser::rebase(const MetadataRebaser& metadataRebaser,
                                  const EntityIDRebaser& entityRebaser,
                                  DataPartRebaser& dataPartRebaser,
                                  const CommitHistory& prevHistory) {
    // Clear the journal: WriteSets may change on reflush after rebase
    _history.journal().clear();

    // TODO: Rebase indexes, don't reset
    static constexpr bool RESET_INDEXES_ON_REBASE = true;
    if (RESET_INDEXES_ON_REBASE) {
        spdlog::warn("Rebase: resetting indexes.");
        _history._validIndexes.clear();
    }
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

    if (prevDataParts.empty()) {
        return;
    }

    const auto* prevPart = prevDataParts.back().get();
    for (auto& part : _history._commitDataparts) {
        dataPartRebaser.rebase(metadataRebaser, *prevPart, *part);
        prevPart = part.get();
    }

    CommitJournal& journal = _history.journal();

    { // Rebase written nodes
        for (NodeID& n : journal.nodeWriteSet()) {
            n = entityRebaser.rebaseNodeID(n);
        }

        for (EdgeID& e : journal.edgeWriteSet()) {
            e = entityRebaser.rebaseEdgeID(e);
        }
    }

    { // Rebase written property types
        for (PropertyTypeID& p : journal.nodePropertyWriteSet()) {
            const PropertyType& remapped = metadataRebaser.getPropertyTypeMapping(p);
            p = remapped._id;
        }

        for (PropertyTypeID& p : journal.edgePropertyWriteSet()) {
            const PropertyType& remapped = metadataRebaser.getPropertyTypeMapping(p);
            p = remapped._id;
        }
    }
}

void CommitHistoryRebaser::removeCreatedDataParts() {
    // Total number of dataparts in the view of this commit
    const size_t totalDPs = _history._allDataparts.size();
    // Total number of datapart which were created as part of this commit, as a result
    // of Change::commit (1 commit = 1 datapart).
    const size_t committedDPs = _history._commitDataparts.size();
    // Just delete the most recent committedDPs number of DPs
    resizeDataParts(totalDPs - committedDPs);
    // Reset this commit to have no locally created DPs
    resetCommitDataParts();
}

void CommitHistoryRebaser::resizeDataParts(size_t newSize) {
    _history._allDataparts.resize(newSize);
}

void CommitHistoryRebaser::resetCommitDataParts() {
    // Set _commitDataparts to be an empty span, but from the same address
    _history._commitDataparts = {_history._commitDataparts.data(), 0};
}
