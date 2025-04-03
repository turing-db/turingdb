#include "MetadataBuilder.h"

#include <mutex>
#include <shared_mutex>

#include "versioning/CommitMetadata.h"

using namespace db;

// Labels
const LabelMap& MetadataBuilder::labels() const {
    std::shared_lock lock {_spinLock};

    return _metadata->_labelMap;
}

LabelID MetadataBuilder::getOrCreateLabel(const std::string& labelName) {
    std::unique_lock lock {_spinLock};

    return _metadata->_labelMap.getOrCreate(labelName);
}

const LabelSetMap& MetadataBuilder::labelsets() const {
    std::shared_lock lock {_spinLock};

    return _metadata->_labelsetMap;
}

LabelSetID MetadataBuilder::getOrCreateLabelSet(const LabelSet& labelset) {
    std::unique_lock lock {_spinLock};

    return _metadata->_labelsetMap.getOrCreate(labelset);
}

// EdgeTypes
const EdgeTypeMap& MetadataBuilder::edgeTypes() const {
    std::shared_lock lock {_spinLock};

    return _metadata->_edgeTypeMap;
}

EdgeTypeID MetadataBuilder::getOrCreateEdgeType(const std::string& edgeTypeName) {
    std::unique_lock lock {_spinLock};

    return _metadata->_edgeTypeMap.getOrCreate(edgeTypeName);
}

// PropertyTypes
const PropertyTypeMap& MetadataBuilder::propTypes() const {
    std::shared_lock lock {_spinLock};

    return _metadata->_propTypeMap;
}

PropertyType MetadataBuilder::getOrCreatePropertyType(const std::string& propTypeName, ValueType valueType) {
    std::unique_lock lock {_spinLock};

    return _metadata->_propTypeMap.getOrCreate(propTypeName, valueType);
}

std::unique_ptr<MetadataBuilder> MetadataBuilder::create(const CommitMetadata& prevMetadata) {
    auto* ptr = new MetadataBuilder;
    ptr->_metadata = std::make_unique<CommitMetadata>(prevMetadata);

    return std::unique_ptr<MetadataBuilder>(ptr);
}
