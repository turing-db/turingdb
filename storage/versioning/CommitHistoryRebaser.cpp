#include "CommitHistoryRebaser.h"

#include <spdlog/spdlog.h>

#include <algorithm>

#include "CommitView.h"
#include "CommitHistory.h"
#include "DataPartRebaser.h"

#include "ID.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/EntityIDRebaser.h"
#include "versioning/MetadataRebaser.h"

#include "indexes/Index.h"
#include "versioning/WriteSet.h"

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

void CommitHistoryRebaser::addValidIndexes(const CommitHistory& prevHistory,
                                           Commit::CommitSpan commitsSinceBranch,
                                           const CommitWriteBuffer::DroppedIndexes& droppedIndexes) {
    std::vector<WeakArc<Index>>& incomingIndexes = _history._validIndexes;

    // Combine the indexes on the change under rebase with the indexes on main
    const CommitHistory::IndexSpan newHeadIndexes = prevHistory.validIndexes();
    for (const WeakArc<Index>& newHeadIndex : newHeadIndexes) {
        const auto findIt = std::ranges::find(incomingIndexes, newHeadIndex);
        const bool alreadyTracked = findIt != end(incomingIndexes);

        if (alreadyTracked) {
            continue;
        }

        const auto droppedIt = std::ranges::find(droppedIndexes, newHeadIndex);
        const bool dropped = droppedIt == std::end(droppedIndexes);

        if (dropped) {
            continue;
        }

        incomingIndexes.push_back(newHeadIndex);
    }

    // Find all the properties that are now invalid due to property updates
    WriteSet<PropertyTypeID> modifiedNodeProperties;
    WriteSet<PropertyTypeID> modifiedEdgeProperties;
    for (const auto& commit : commitsSinceBranch) {
        const CommitHistory& pastHistory = commit->history();
        const CommitJournal& pastJournal = pastHistory.journal();

        const WriteSet<PropertyTypeID>& pastNodeProps = pastJournal.nodePropertyWriteSet();
        const WriteSet<PropertyTypeID>& pastEdgeProps = pastJournal.edgePropertyWriteSet();

        WriteSet<PropertyTypeID>::setUnion(modifiedNodeProperties, pastNodeProps);
        WriteSet<PropertyTypeID>::setUnion(modifiedEdgeProperties, pastEdgeProps);
    }

    const auto invalidatedIndex = [&](const WeakArc<Index>& index){
        const bool isNode = index->isNodeIndex();
        const PropertyTypeID indexedProp = index->property();

        const WriteSet<PropertyTypeID>& invalidSet =
            isNode ? modifiedNodeProperties : modifiedEdgeProperties;

        return invalidSet.contains(indexedProp);
    };

    // Remove indexes that are now invalid
    std::erase_if(incomingIndexes, invalidatedIndex);
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
