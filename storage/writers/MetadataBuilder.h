#pragma once

#include <memory>

#include "EntityID.h"
#include "RWSpinLock.h"
#include "labels/LabelSetHandle.h"
#include "types/PropertyType.h"


namespace db {

class CommitMetadata;

class MetadataBuilder {
public:
    // Labels
    LabelID getOrCreateLabel(const std::string& labelName);

    // Labelsets
    LabelSetHandle getOrCreateLabelSet(const LabelSet& labelset);

    // EdgeTypes
    EdgeTypeID getOrCreateEdgeType(const std::string& edgeTypeName);

    // PropertyTypes
    PropertyType getOrCreatePropertyType(const std::string& propTypeName, ValueType valueType);

    [[nodiscard]] static std::unique_ptr<MetadataBuilder> create(const CommitMetadata& prevMetadata, CommitMetadata* metadata);

private:
    mutable RWSpinLock _spinLock;
    CommitMetadata* _metadata {nullptr};

    MetadataBuilder() = default;
};

}
