#include "EmbeddingDimensionChecker.h"

#include <algorithm>
#include <optional>
#include <string>
#include <variant>

#include <spdlog/fmt/fmt.h>

#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"
#include "properties/PropertyManager.h"
#include "reader/GraphReader.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/VersionControlException.h"
#include "views/GraphView.h"

#include "BioAssert.h"

using namespace db;

namespace {

std::optional<size_t> embeddingDimensionOf(const CommitWriteBuffer::UntypedProperty& property) {
    using OptionalEmbedding = std::optional<types::Embedding::OwningPrimitive>;

    const OptionalEmbedding* embedding = std::get_if<OptionalEmbedding>(&property.value);

    const bool holdsEmbedding = embedding && embedding->has_value();
    if (!holdsEmbedding) {
        return std::nullopt;
    }

    return (*embedding)->size();
}

}

EmbeddingDimensionChecker::EmbeddingDimensionChecker(const PropertyTypeMap& writtenTypes, const GraphView& graph)
    : _graph(graph),
    _rebasedTypes(graph.metadata().propTypes())
{
    _rebaser.mapPropertyTypes(writtenTypes, _rebasedTypes);
}

EmbeddingDimensionChecker::~EmbeddingDimensionChecker() {
}

void EmbeddingDimensionChecker::addWrites(const PropertyManager& properties) {
    for (const auto& [ptID, container] : properties) {
        const std::optional<size_t> dimension = properties.findEmbeddingDimension(ptID);
        if (dimension) {
            addWrite(ptID, *dimension);
        }
    }
}

void EmbeddingDimensionChecker::addWrites(const CommitWriteBuffer& writeBuffer) {
    const auto addProperty = [this](const CommitWriteBuffer::UntypedProperty& property) {
        const std::optional<size_t> dimension = embeddingDimensionOf(property);
        if (dimension) {
            addWrite(property.propertyID, *dimension);
        }
    };

    for (const CommitWriteBuffer::PendingNode& node : writeBuffer.pendingNodes()) {
        std::ranges::for_each(node.properties, addProperty);
    }

    for (const CommitWriteBuffer::PendingEdge& edge : writeBuffer.pendingEdges()) {
        std::ranges::for_each(edge.properties, addProperty);
    }

    for (const CommitWriteBuffer::NodeUpdate& update : writeBuffer.updatedNodes()) {
        addProperty(update._updatedValue);
    }

    for (const CommitWriteBuffer::EdgeUpdate& update : writeBuffer.updatedEdges()) {
        addProperty(update._updatedValue);
    }
}

void EmbeddingDimensionChecker::check() const {
    const GraphReader reader = _graph.read();

    for (const auto& [ptID, dimension] : _dimensions) {
        const std::optional<size_t> heldDimension = reader.findEmbeddingDimension(ptID);

        const bool differs = heldDimension.has_value() && *heldDimension != dimension;
        if (differs) {
            throwDimensionError(getPropertyName(ptID), *heldDimension, dimension);
        }
    }
}

void EmbeddingDimensionChecker::throwDimensionError(std::string_view propertyName,
                                                    size_t heldDimension,
                                                    size_t writtenDimension) {
    throw VersionControlException(fmt::format("Embedding property '{}' has dimension {} and cannot take an embedding of dimension {}",
                                              propertyName,
                                              heldDimension,
                                              writtenDimension));
}

void EmbeddingDimensionChecker::addWrite(PropertyTypeID ptID, size_t dimension) {
    const PropertyType rebased = _rebaser.getPropertyTypeMapping(ptID);
    const auto [it, inserted] = _dimensions.emplace(rebased._id, dimension);

    const bool differs = !inserted && it->second != dimension;
    if (differs) {
        throwDimensionError(getPropertyName(rebased._id), it->second, dimension);
    }
}

std::string_view EmbeddingDimensionChecker::getPropertyName(PropertyTypeID ptID) const {
    const std::optional<std::string_view> name = _rebasedTypes.getName(ptID);
    bioassert(name.has_value(), "An embedding write names a property the metadata does not hold");

    return *name;
}
