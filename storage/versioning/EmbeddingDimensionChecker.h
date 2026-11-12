#pragma once

#include <stddef.h>

#include <string_view>
#include <unordered_map>

#include "ID.h"
#include "metadata/PropertyTypeMap.h"
#include "versioning/MetadataRebaser.h"
#include "views/GraphView.h"

namespace db {

class CommitWriteBuffer;
class PropertyManager;

class EmbeddingDimensionChecker {
public:
    EmbeddingDimensionChecker(const PropertyTypeMap& writtenTypes, const GraphView& graph);
    ~EmbeddingDimensionChecker();

    EmbeddingDimensionChecker(const EmbeddingDimensionChecker&) = delete;
    EmbeddingDimensionChecker(EmbeddingDimensionChecker&&) = delete;
    EmbeddingDimensionChecker& operator=(const EmbeddingDimensionChecker&) = delete;
    EmbeddingDimensionChecker& operator=(EmbeddingDimensionChecker&&) = delete;

    void addWrites(const PropertyManager& properties);
    void addWrites(const CommitWriteBuffer& writeBuffer);

    void check() const;

    [[noreturn]] static void throwDimensionError(std::string_view propertyName,
                                                 size_t heldDimension,
                                                 size_t writtenDimension);

private:
    GraphView _graph;
    PropertyTypeMap _rebasedTypes;
    MetadataRebaser _rebaser;
    std::unordered_map<PropertyTypeID, size_t> _dimensions;

    void addWrite(PropertyTypeID ptID, size_t dimension);
    std::string_view getPropertyName(PropertyTypeID ptID) const;
};

}
