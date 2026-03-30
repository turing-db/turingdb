#include "CommitHistoryRebaser.h"

#include <spdlog/spdlog.h>

#include "CommitView.h"
#include "CommitHistory.h"
#include "DataPartRebaser.h"

#include "versioning/EntityIDRebaser.h"
#include "versioning/MetadataRebaser.h"

#include "indexes/Index.h"

using namespace db;

void CommitHistoryRebaser::rebase(const MetadataRebaser& metadataRebaser,
                                  const EntityIDRebaser& entityRebaser,
                                  DataPartRebaser& dataPartRebaser,
                                  const CommitHistory& prevHistory) {
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

    // FIXME: this is wrong, need to walk commits added
    { // Only carry forward indexes that are still valid
        const CommitJournal& journal = prevHistory.journal();
        const auto& nodePropUpdates = journal.nodePropertyWriteSet();
        const auto& edgePropUpdates = journal.edgePropertyWriteSet();

        for (const WeakArc<Index>& prevIndex : prevHistory.validIndexes()) {
            const bool isNode = prevIndex->isNodeIndex();
            const PropertyTypeID indexedProp = prevIndex->property();
            const WriteSet<PropertyTypeID>& propUpdates =
                isNode ? nodePropUpdates : edgePropUpdates;
            const bool propertyInvalidated = propUpdates.contains(indexedProp);

            if (!propertyInvalidated) {
                _history._validIndexes.push_back(prevIndex);
            } else {
                spdlog::warn("Dropping index {} because property {} was updated.",
                             prevIndex->name(), indexedProp.getValue());
            }
        }
    }

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
