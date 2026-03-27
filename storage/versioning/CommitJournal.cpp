#include "CommitJournal.h"

#include <memory>

using namespace db;

std::unique_ptr<CommitJournal> CommitJournal::emptyJournal() {
    CommitJournal* journal = new CommitJournal;
    return std::unique_ptr<CommitJournal>(journal);
}

void CommitJournal::clear() {
    _nodeWriteSet.clear();
    _edgeWriteSet.clear();
}

bool CommitJournal::empty() const {
    return _nodeWriteSet.empty() && _edgeWriteSet.empty();
}

void CommitJournal::addWrittenNode(NodeID node) {
    _nodeWriteSet.insert(node);
}

void CommitJournal::addWrittenEdge(EdgeID edge) {
    _edgeWriteSet.insert(edge);
}

void CommitJournal::addWrittenNodeProperty(PropertyTypeID prop) {
    _nodePropertyWriteSet.insert(prop);
}

void CommitJournal::addWrittenEdgeProperty(PropertyTypeID prop) {
    _edgePropertyWriteSet.insert(prop);
}

void CommitJournal::finalise() {
    _nodeWriteSet.finalise();
    _edgeWriteSet.finalise();
}

