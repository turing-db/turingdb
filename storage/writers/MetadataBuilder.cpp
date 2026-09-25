#include "MetadataBuilder.h"

#include <mutex>
#include <shared_mutex>

#include "Profiler.h"
#include "metadata/LabelMap.h"
#include "metadata/LabelSetMap.h"
#include "metadata/EdgeTypeMap.h"
#include "metadata/PropertyTypeMap.h"
#include "metadata/GraphMetadata.h"

using namespace db;

LabelID MetadataBuilder::getOrCreateLabel(std::string_view labelName) {
    std::unique_lock lock(_spinLock);

    return _metadata->_labelMap.getOrCreate(labelName);
}

std::optional<LabelID> MetadataBuilder::findLabel(std::string_view labelName) const {
    std::shared_lock lock(_spinLock);

    return _metadata->_labelMap.get(labelName);
}

LabelSetHandle MetadataBuilder::getOrCreateLabelSet(const LabelSet& labelset) {
    std::unique_lock lock(_spinLock);

    return _metadata->_labelsetMap.getOrCreate(labelset);
}

void MetadataBuilder::forEachLabelSet(const LabelSetVisitor& visit) const {
    std::shared_lock lock(_spinLock);

    for (const LabelSetMap::Pair& pair : _metadata->labelsets()) {
        visit(pair._id, *pair._value);
    }
}

EdgeTypeID MetadataBuilder::getOrCreateEdgeType(std::string_view edgeTypeName) {
    std::unique_lock lock(_spinLock);

    return _metadata->_edgeTypeMap.getOrCreate(edgeTypeName);
}

std::optional<EdgeTypeID> MetadataBuilder::findEdgeType(std::string_view edgeTypeName) const {
    std::shared_lock lock(_spinLock);

    return _metadata->_edgeTypeMap.get(edgeTypeName);
}

PropertyType MetadataBuilder::getOrCreatePropertyType(std::string_view propTypeName, ValueType valueType) {
    std::unique_lock lock(_spinLock);

    return  _metadata->_propTypeMap.getOrCreate(propTypeName, valueType);
}

std::optional<PropertyType> MetadataBuilder::findPropertyType(std::string_view propTypeName) const {
    std::shared_lock lock(_spinLock);

    return _metadata->_propTypeMap.get(propTypeName);
}

void MetadataBuilder::beginStatement() {
    std::shared_lock lock(_spinLock);

    _statementLabels = _metadata->_labelMap.getCount();
    _statementLabelSets = _metadata->_labelsetMap.getCount();
    _statementEdgeTypes = _metadata->_edgeTypeMap.getCount();
    _statementPropertyTypes = _metadata->_propTypeMap.getCount();
}

void MetadataBuilder::rollbackStatement() {
    std::unique_lock lock(_spinLock);

    _metadata->_labelMap.truncate(_statementLabels);
    _metadata->_labelsetMap.truncate(_statementLabelSets);
    _metadata->_edgeTypeMap.truncate(_statementEdgeTypes);
    _metadata->_propTypeMap.truncate(_statementPropertyTypes);
}

std::unique_ptr<MetadataBuilder> MetadataBuilder::create(const GraphMetadata& prevMetadata, GraphMetadata* metadata) {
    Profile profile("MetadataBuilder::create");

    auto* ptr = new MetadataBuilder;
    ptr->_metadata = metadata;

    return std::unique_ptr<MetadataBuilder>(ptr);
}
