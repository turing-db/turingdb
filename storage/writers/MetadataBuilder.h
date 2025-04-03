#pragma once

#include <memory>

#include "EntityID.h"
#include "RWSpinLock.h"
#include "labels/LabelSet.h"
#include "types/PropertyType.h"
#include "labels/LabelMap.h"
#include "labels/LabelSetMap.h"
#include "types/EdgeTypeMap.h"
#include "types/PropertyTypeMap.h"


namespace db {

class CommitMetadata;

class MetadataBuilder {
public:
    // Labels
    [[nodiscard]] const LabelMap& labels() const;
    LabelID getOrCreateLabel(const std::string& labelName);

    // Labelsets
    [[nodiscard]] const LabelSetMap& labelsets() const;
    LabelSetID getOrCreateLabelSet(const LabelSet& labelset);

    // EdgeTypes
    [[nodiscard]] const EdgeTypeMap& edgeTypes() const;
    EdgeTypeID getOrCreateEdgeType(const std::string& edgeTypeName);

    // PropertyTypes
    [[nodiscard]] const PropertyTypeMap& propTypes() const;
    PropertyType getOrCreatePropertyType(const std::string& propTypeName, ValueType valueType);

    [[nodiscard]] static std::unique_ptr<MetadataBuilder> create(const CommitMetadata& prevMetadata);

private:
    mutable RWSpinLock _spinLock;
    std::unique_ptr<CommitMetadata> _metadata;

    MetadataBuilder() = default;
};

}
