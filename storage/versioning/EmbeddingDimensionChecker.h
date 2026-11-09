#pragma once

#include <stddef.h>

#include <string_view>
#include <unordered_map>

#include "ID.h"

namespace db {

class CommitWriteBuffer;
class GraphView;
class PropertyManager;
class PropertyTypeMap;

class EmbeddingDimensionChecker {
public:
    explicit EmbeddingDimensionChecker(const PropertyTypeMap& propertyTypes);
    ~EmbeddingDimensionChecker();

    EmbeddingDimensionChecker(const EmbeddingDimensionChecker&) = delete;
    EmbeddingDimensionChecker(EmbeddingDimensionChecker&&) = delete;
    EmbeddingDimensionChecker& operator=(const EmbeddingDimensionChecker&) = delete;
    EmbeddingDimensionChecker& operator=(EmbeddingDimensionChecker&&) = delete;

    void addWrites(const PropertyManager& properties);
    void addWrites(const CommitWriteBuffer& writeBuffer);

    void checkAgainst(const GraphView& graph) const;

    [[noreturn]] static void throwDimensionError(std::string_view propertyName,
                                                 size_t heldDimension,
                                                 size_t writtenDimension);

private:
    const PropertyTypeMap& _propertyTypes;
    std::unordered_map<PropertyTypeID, size_t> _dimensions;

    void addWrite(PropertyTypeID ptID, size_t dimension);
    std::string_view getPropertyName(PropertyTypeID ptID) const;
};

}
