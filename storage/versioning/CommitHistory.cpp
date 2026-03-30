#include "CommitHistory.h"

#include <spdlog/spdlog.h>

#include "CommitHistoryRebaser.h"
#include "CommitView.h"
#include "versioning/CommitJournal.h"

#include "indexes/Index.h"
#include "versioning/WriteSet.h"

using namespace db;

CommitHistory::CommitHistory() = default;

CommitHistory::~CommitHistory() = default;

void CommitHistory::newCommitHistoryFromPrevious(const CommitHistory& previous) {
    _allDataparts = previous._allDataparts;
    _commitDataparts = {};

    const CommitJournal& journal = previous.journal();
    const auto& nodePropUpdates = journal.nodePropertyWriteSet();
    const auto& edgePropUpdates = journal.edgePropertyWriteSet();

    for (const WeakArc<Index>& prevIndex : previous.validIndexes()) {
        const bool isNode = prevIndex->isNodeIndex();
        const PropertyTypeID indexedProp = prevIndex->property();
        const WriteSet<PropertyTypeID>& propUpdates = isNode ? nodePropUpdates : edgePropUpdates;
        const bool propertyInvalidated = propUpdates.contains(indexedProp);

        if (!propertyInvalidated) {
            _validIndexes.push_back(prevIndex);
        } else {
            spdlog::warn("Dropping index {} because property {} was updated.",
                         prevIndex->name(), indexedProp.getValue());
        }
    }
}

void CommitHistory::newMergeCommitHistory() {
    _allDataparts = {};
    _commitDataparts = {};
    _validIndexes = {};
}
