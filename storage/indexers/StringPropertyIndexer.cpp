#include "StringPropertyIndexer.h"

#include "indexes/StringIndex.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyContainer.h"
#include "ID.h"

using namespace db;

void StringPropertyIndexer::initialiseIndexTrie(PropertyTypeID propertyID) {
    // Initialise trie at key if not already there
    _indexer.try_emplace(propertyID, std::make_unique<StringIndex>());
}

void StringPropertyIndexer::addStringPropertyToIndex(PropertyTypeID propertyID,
                                                     const TypedPropertyContainer<types::String>& stringPropertyContainer) {
    // Get the index map for this property type

    StringIndex* trie = _indexer.at(propertyID).get();
    if (!trie) {
        throw TuringException("Tree is nullpointer at property index "
                              + std::to_string(propertyID.getValue()));
    }

    // Get [ID, stringValue] pairs
    const auto zipped = stringPropertyContainer.zipped();
    std::vector<std::string> tokens;
    for (const auto&& [id, stringValue] : zipped) {
        // Preprocess and tokenise the string into alphanumeric subwords
        StringIndex::preprocess(tokens, stringValue);
        // Insert each subword
        for (const auto& token : tokens) {
            trie->insert(token, id.getValue());
        }
        tokens.clear();
    }
}

void StringPropertyIndexer::buildIndex(
    std::vector<std::pair<PropertyTypeID, PropertyContainer*>>& toIndex) {
    // Initialise tries for all present string property IDs
    for (const auto& [ptID, _] : toIndex) {
        initialiseIndexTrie(ptID);
    }

    for (const auto& [ptID, props] : toIndex) {
        const TypedPropertyContainer<types::String>& strPropContainer =
            props->cast<types::String>();
        addStringPropertyToIndex(ptID, strPropContainer);
    }
}
