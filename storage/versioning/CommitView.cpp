#include "CommitView.h"
#include "versioning/Commit.h"

using namespace db;

bool CommitView::isValid() const {
    return _commit != nullptr;
}

bool CommitView::hasData() const {
    return _commit->hasData();
}

bool CommitView::isHead() const {
    return _commit->isHead();
}

CommitHash CommitView::hash() const {
    return _commit->hash();
}

DataPartSpan CommitView::dataparts() const {
    return _commit->data().commitDataparts();
}


