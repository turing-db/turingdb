#include "GraphView.h"

#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"


using namespace db;

GraphReader GraphView::read() const {
    return GraphReader(*this);
}

std::unique_ptr<CommitBuilder> GraphView::prepareCommit() const {
    return CommitBuilder::prepare(*_graph, *this);
}

