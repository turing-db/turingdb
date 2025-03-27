#include "Commit.h"

#include "Graph.h"

using namespace db;

Commit::Commit() = default;

Commit::~Commit() = default;

bool Commit::isHead() const {
    return _graph->getHeadHash() == _hash;
}
