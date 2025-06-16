#pragma once

#include <memory>

#include "ID.h"
#include "RWSpinLock.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"


namespace db {

class GraphMetadata;
class MetadataRebaser;

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

    [[nodiscard]] static std::unique_ptr<MetadataBuilder> create(const GraphMetadata& prevMetadata, GraphMetadata* metadata);

private:
    friend class MetadataRebaser;

    mutable RWSpinLock _spinLock;
    GraphMetadata* _metadata {nullptr};

    MetadataBuilder() = default;
};

}
