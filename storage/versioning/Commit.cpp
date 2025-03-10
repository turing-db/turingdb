#include "Commit.h"

#include "CommitData.h"
#include "views/GraphView.h"

using namespace db;

Commit::Commit() = default;

Commit::~Commit() = default;

GraphView Commit::view() const {
    return GraphView {*_graph, _data};
}

