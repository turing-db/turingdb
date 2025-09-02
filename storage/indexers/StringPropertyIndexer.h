#pragma once

#include <memory>

#include "ID.h"
#include "properties/PropertyContainer.h"
#include "indexes/StringIndex.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyContainer.h"

namespace db {

class StringPropertyIndexer {
private:
    template <TypedInternalID IDT>
    using TempIDMap = std::unordered_map<IDT, IDT>;

public:
    StringPropertyIndexer() = default;

    bool addIndex(PropertyTypeID id, std::unique_ptr<StringIndex>&& idx) {
        return _indexer.try_emplace(id, std::move(idx)).second;
    }

    template <TypedInternalID IDT>
    void buildIndex(std::vector<std::pair<PropertyTypeID, PropertyContainer*>>& toIndex,
                    const TempIDMap<IDT>& tempIDMap);

    bool contains(PropertyTypeID propID) const { return _indexer.contains(propID); }

    const std::unique_ptr<StringIndex>& at(PropertyTypeID propID) const { return _indexer.at(propID); }

    size_t size() const { return _indexer.size(); }

    void setInitialised() { _initialised = true; }

    bool isInitialised() const { return _initialised; }

    auto begin() const { return _indexer.begin(); }

    auto end() const { return _indexer.end(); }

    const auto find(PropertyTypeID id) const { return _indexer.find(id); }

private:
    std::map<PropertyTypeID, std::unique_ptr<StringIndex>> _indexer;
    bool _initialised {false};

    void initialiseIndexTrie(PropertyTypeID propertyID);

    template <TypedInternalID IDT>
    void addStringPropertyToIndex(
        PropertyTypeID propertyID,
        const TypedPropertyContainer<types::String>& stringPropertyContainer,
        const TempIDMap<IDT>& tempIDMap);
};

template <TypedInternalID IDT>
void StringPropertyIndexer::buildIndex(
    std::vector<std::pair<PropertyTypeID, PropertyContainer*>>& toIndex,
    const TempIDMap<IDT>& tempIDMap) {
    // Initialise tries for all present string property IDs
    for (const auto& [ptID, _] : toIndex) {
        initialiseIndexTrie(ptID);
    }

    for (const auto& [ptID, props] : toIndex) {
        const TypedPropertyContainer<types::String>& strPropContainer =
            props->template cast<types::String>();
        addStringPropertyToIndex(ptID, strPropContainer, tempIDMap);
    }
}

template <TypedInternalID IDT>
void StringPropertyIndexer::addStringPropertyToIndex(
    PropertyTypeID propertyID,
    const TypedPropertyContainer<types::String>& stringPropertyContainer,
    const TempIDMap<IDT>& tempIDMap) {
    // Get the index map for this property type
    StringIndex* trie = _indexer.at(propertyID).get();
    if (!trie) {
        throw TuringException("Tree is nullpointer at property index "
                              + std::to_string(propertyID.getValue()));
    }

    // Get [ID, stringValue] pairs
    const auto zipped = stringPropertyContainer.zipped();
    std::vector<std::string> tokens;
    for (const auto&& [tId, stringValue] : zipped) {
        auto id = tempIDMap.at(tId.getValue());
        // Preprocess and tokenise the string into alphanumeric subwords
        StringIndex::preprocess(tokens, stringValue);
        // Insert each subword
        for (const auto& token : tokens) {
            trie->insert(token, id.getValue());
        }
        tokens.clear();
    }
}

}
