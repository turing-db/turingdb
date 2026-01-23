#include "CommitView.h"

#include "versioning/Commit.h"
#include "versioning/Tombstones.h"
#include "versioning/Transaction.h"

using namespace db;

bool CommitView::isValid() const {
    return _commit != nullptr;
}

bool CommitView::hasData() const {
    return _commit->hasData();
}

const CommitData& CommitView::data() const {
    return _commit->data();
}

bool CommitView::isHead() const {
    return _commit->isHead();
}

CommitHash CommitView::hash() const {
    return _commit->hash();
}

const VersionController& CommitView::controller() const {
    return _commit->controller();
}

DataPartSpan CommitView::dataparts() const {
    return _commit->data().commitDataparts();
}

const CommitHistory& CommitView::history() const {
    return _commit->history();
}

const GraphMetadata& CommitView::metadata() const {
    return _commit->data().metadata();
}

const Tombstones& CommitView::tombstones() const {
    return _commit->data().tombstones();
}

FrozenCommitTx CommitView::openTransaction() const {
    return _commit->openTransaction();
}
