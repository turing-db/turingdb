#pragma once

#include <memory>
#include <optional>

#include "ID.h"
#include "RWSpinLock.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"

namespace db {

class GraphMetadata;
class LabelSetMap;
class MetadataRebaser;

class MetadataBuilder {
public:
    // Labels
    LabelID getOrCreateLabel(std::string_view labelName);
    [[nodiscard]] std::optional<LabelID> findLabel(std::string_view labelName) const;

    // Labelsets
    LabelSetHandle getOrCreateLabelSet(const LabelSet& labelset);

    // Every label set this change knows: the ones the graph carried when it opened, and
    // the ones it has interned since
    [[nodiscard]] const LabelSetMap& labelsets() const;

    // EdgeTypes
    EdgeTypeID getOrCreateEdgeType(std::string_view edgeTypeName);
    [[nodiscard]] std::optional<EdgeTypeID> findEdgeType(std::string_view edgeTypeName) const;

    // PropertyTypes
    PropertyType getOrCreatePropertyType(std::string_view propTypeName, ValueType valueType);

    // What this change knows a property name by: what the graph carried when the change
    // opened, plus what the change has written since. Empty for a name neither holds
    [[nodiscard]] std::optional<PropertyType> findPropertyType(std::string_view propTypeName) const;

    [[nodiscard]] static std::unique_ptr<MetadataBuilder> create(const GraphMetadata& prevMetadata, GraphMetadata* metadata);

private:
    friend class MetadataRebaser;

    mutable RWSpinLock _spinLock;
    GraphMetadata* _metadata {nullptr};

    MetadataBuilder() = default;
};

}
