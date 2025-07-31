#pragma once

#include <memory>
#include <unordered_map>

#include "ID.h"
#include "properties/PropertyContainer.h"
#include "indexes/StringIndex.h"

namespace db {

class PropertyContainer;

class StringPropertyIndexer {
public:
    StringPropertyIndexer() = default;
    
    void buildIndex(std::vector<std::pair<PropertyTypeID, PropertyContainer*>>& toIndex);

    bool contains(PropertyTypeID propID) const { return _indexer.contains(propID); }

    const std::unique_ptr<StringIndex>& at(PropertyTypeID propID) const { return _indexer.at(propID); }

    size_t size() const { return _indexer.size(); }

    void setInitialised() { _initialised = true; }

    bool isInitialised() const { return _initialised; }

private:
    std::unordered_map<PropertyTypeID, std::unique_ptr<StringIndex>> _indexer {};
    bool _initialised {false};

    void initialiseIndexTrie(PropertyTypeID propertyID);
    void addStringPropertyToIndex(
        PropertyTypeID propertyID,
        const TypedPropertyContainer<types::String>& stringPropertyContainer);
};
}
