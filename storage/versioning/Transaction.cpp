#include "Transaction.h"

#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"

using namespace db;

Transaction::Transaction() = default;

Transaction::~Transaction() = default;

GraphView Transaction::viewGraph() const {
    return GraphView {*_data};
}

GraphReader Transaction::readGraph() const {
    return GraphView {*_data}.read();
}

GraphView WriteTransaction::viewGraph() const {
    return GraphView {*_data};
}

GraphReader WriteTransaction::readGraph() const {
    return GraphView {*_data}.read();
}

std::unique_ptr<CommitBuilder> WriteTransaction::prepareCommit() const {
    return CommitBuilder::prepare(*_graph, viewGraph());
}
