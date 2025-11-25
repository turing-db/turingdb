#include "MetadataBuilder.h"

#include <mutex>

#include "Profiler.h"
#include "metadata/LabelMap.h"
#include "metadata/LabelSetMap.h"
#include "metadata/EdgeTypeMap.h"
#include "metadata/PropertyTypeMap.h"
#include "metadata/GraphMetadata.h"

using namespace db;

LabelID MetadataBuilder::getOrCreateLabel(std::string_view labelName) {
    std::unique_lock lock {_spinLock};

    return _metadata->_labelMap.getOrCreate(labelName);
}

LabelSetHandle MetadataBuilder::getOrCreateLabelSet(const LabelSet& labelset) {
    std::unique_lock lock {_spinLock};

    return _metadata->_labelsetMap.getOrCreate(labelset);
}

EdgeTypeID MetadataBuilder::getOrCreateEdgeType(const std::string& edgeTypeName) {
    std::unique_lock lock {_spinLock};

    return _metadata->_edgeTypeMap.getOrCreate(edgeTypeName);
}

PropertyType MetadataBuilder::getOrCreatePropertyType(const std::string& propTypeName, ValueType valueType) {
    std::unique_lock lock {_spinLock};

    return  _metadata->_propTypeMap.getOrCreate(propTypeName, valueType);
}

std::unique_ptr<MetadataBuilder> MetadataBuilder::create(const GraphMetadata& prevMetadata, GraphMetadata* metadata) {
    Profile profile {"MetadataBuilder::create"};

    auto* ptr = new MetadataBuilder;
    ptr->_metadata = metadata;

    return std::unique_ptr<MetadataBuilder>(ptr);
}
