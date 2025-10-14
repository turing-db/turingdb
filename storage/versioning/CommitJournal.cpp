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

bool CommitJournal::empty() {
    return _nodeWriteSet.empty() && _nodeWriteSet.empty();
}

void CommitJournal::addWrittenNode(NodeID node) {
    _nodeWriteSet.insert(node);
}

void CommitJournal::addWrittenEdge(EdgeID edge) {
    _edgeWriteSet.insert(edge);
}

void CommitJournal::addWrittenNodes(const std::vector<NodeID>& nodes) {
    _nodeWriteSet.insert(nodes);
}

void CommitJournal::addWrittenEdges(const std::vector<EdgeID>& edges) {
    _edgeWriteSet.insert(edges);
}

void CommitJournal::finalise() {
    _nodeWriteSet.finalise();
    _edgeWriteSet.finalise();
}

