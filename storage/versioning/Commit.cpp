#include "Commit.h"

#include "VersionController.h"
#include "Transaction.h"

using namespace db;

Commit::Commit() = default;

Commit::~Commit() = default;

bool Commit::isHead() const {
    return _controller->getHeadHash() == _hash;
}

Transaction Commit::openTransaction() const {
    return Transaction {_data};
}
