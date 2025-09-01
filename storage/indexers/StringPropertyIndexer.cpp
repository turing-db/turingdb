#include "StringPropertyIndexer.h"

#include "indexes/StringIndex.h"

#include "ID.h"

using namespace db;

void StringPropertyIndexer::initialiseIndexTrie(PropertyTypeID propertyID) {
    // Initialise trie at key if not already there
    _indexer.try_emplace(propertyID, std::make_unique<StringIndex>());
}
