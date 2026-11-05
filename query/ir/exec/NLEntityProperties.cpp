#include "NLEntityProperties.h"

#include <algorithm>
#include <type_traits>

#include <range/v3/view/reverse.hpp>

#include "datapart/DataPart.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyTypeMap.h"
#include "properties/PropertyManager.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "NLExecutionContext.h"
#include "NLValueStore.h"
#include "NLWriteProperties.h"
#include "NLWrittenValues.h"

#include "BioAssert.h"

using namespace db;

namespace rv = ranges::views;

namespace {

MapBuffer<>::MapItemVariant storedValue(const PropertyContainer& container, EntityID entity) {
    switch (container.getValueType()) {
        case ValueType::Int64:
            return container.cast<types::Int64>().get(entity);
        break;
        case ValueType::UInt64:
            return container.cast<types::UInt64>().get(entity);
        break;
        case ValueType::Double:
            return container.cast<types::Double>().get(entity);
        break;
        case ValueType::String:
            return container.cast<types::String>().get(entity);
        break;
        case ValueType::Bool:
            return container.cast<types::Bool>().get(entity);
        break;
        case ValueType::Embedding:
            return container.cast<types::Embedding>().get(entity);
        break;
        case ValueType::List:
            return container.cast<types::List>().get(entity);
        break;
        case ValueType::DateTime:
            return container.cast<types::DateTime>().get(entity);
        break;
        case ValueType::Map:
            return container.cast<types::Map>().get(entity);
        break;
        case ValueType::Duration:
            return container.cast<types::Duration>().get(entity);
        break;
        case ValueType::Point:
            return container.cast<types::Point>().get(entity);
        break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
        break;
    }

    bioassert(false, "Property container of an invalid value type");
    return PropertyNull {};
}

template <SupportedType T>
MapBuffer<>::MapItemVariant readWritten(NLWrittenValues& written,
                                        NLValueStore& readValues,
                                        const NLWrittenValues::Value& value) {
    const std::optional<typename T::Primitive> read = written.read<T>(value, readValues);
    if (!read) {
        return PropertyNull {};
    }

    return *read;
}

MapBuffer<>::MapItemVariant writtenValue(NLWrittenValues& written,
                                         NLValueStore& readValues,
                                         ValueType valueType,
                                         const NLWrittenValues::Value& value) {
    switch (valueType) {
        case ValueType::Int64:
            return readWritten<types::Int64>(written, readValues, value);
        break;
        case ValueType::UInt64:
            return readWritten<types::UInt64>(written, readValues, value);
        break;
        case ValueType::Double:
            return readWritten<types::Double>(written, readValues, value);
        break;
        case ValueType::String:
            return readWritten<types::String>(written, readValues, value);
        break;
        case ValueType::Bool:
            return readWritten<types::Bool>(written, readValues, value);
        break;
        case ValueType::Embedding:
            return readWritten<types::Embedding>(written, readValues, value);
        break;
        case ValueType::List:
            return readWritten<types::List>(written, readValues, value);
        break;
        case ValueType::DateTime:
            return readWritten<types::DateTime>(written, readValues, value);
        break;
        case ValueType::Map:
            return readWritten<types::Map>(written, readValues, value);
        break;
        case ValueType::Duration:
            return readWritten<types::Duration>(written, readValues, value);
        break;
        case ValueType::Point:
            return readWritten<types::Point>(written, readValues, value);
        break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
        break;
    }

    bioassert(false, "Written property of an invalid value type");
    return PropertyNull {};
}

template <TypedInternalID IDT>
const PropertyManager& partProperties(const DataPart& part) {
    if constexpr (std::is_same_v<IDT, NodeID>) {
        return part.nodeProperties();
    } else {
        return part.edgeProperties();
    }
}

template <TypedInternalID IDT>
IDT firstPartEntity(const DataPart& part) {
    if constexpr (std::is_same_v<IDT, NodeID>) {
        return part.getFirstNodeID();
    } else {
        return part.getFirstEdgeID();
    }
}

}

NLEntityProperties::NLEntityProperties(NLExecutionContext* context, NLValueStore* readValues)
    : _view(context->getView()),
    _writeBuffer(context->getWriteBuffer()),
    _written(&context->getWrittenValues()),
    _readValues(readValues),
    _metadataBuilder(context->getMetadataBuilder())
{
    if (_writeBuffer) {
        _written->indexUpdates(_writeBuffer);

        _committedNodeCount = committedNodeCount(_view);
        _committedEdgeCount = committedEdgeCount(_view);
    }
}

NLEntityProperties::~NLEntityProperties() {
}

template <TypedInternalID IDT>
void NLEntityProperties::readKeys(IDT entity, std::vector<std::string_view>& keys) {
    _readsValues = false;
    collect(entity);

    keys.clear();
    for (const Property& property : _properties) {
        keys.push_back(property._name);
    }
}

template <TypedInternalID IDT>
void NLEntityProperties::readIDs(IDT entity, std::vector<PropertyTypeID>& ids) {
    _readsValues = false;
    collect(entity);

    ids.clear();
    for (const Property& property : _properties) {
        ids.push_back(property._id);
    }
}

template <TypedInternalID IDT>
void NLEntityProperties::readEntries(IDT entity, std::vector<Entry>& entries) {
    _readsValues = true;
    collect(entity);

    entries.clear();
    for (const Property& property : _properties) {
        entries.push_back(Entry {.key = property._name, .value = property._value});
    }
}

template <TypedInternalID IDT>
bool NLEntityProperties::readValue(IDT entity, std::string_view name, MapBuffer<>::MapItemVariant& value) {
    const PropertyType type = findPropertyType(name);

    if (!type.isValid()) {
        return false;
    } else if (isPending(entity)) {
        return readPendingValue(entity, type, value);
    } else if (const CommitWriteBuffer::SupportedTypeVariant* update = findUpdate(entity, type._id)) {
        return readWrittenValue(type, *update, value);
    }

    return readCommittedValue(entity, type._id, value);
}

template <TypedInternalID IDT>
void NLEntityProperties::collect(IDT entity) {
    _properties.clear();

    if (isPending(entity)) {
        collectPending(entity);
    } else {
        collectCommitted(entity);
        collectUpdates(entity);
    }

    std::ranges::sort(_properties, {}, &Property::_name);
}

template <TypedInternalID IDT>
void NLEntityProperties::collectCommitted(IDT entity) {
    const EntityID entityID {entity.getValue()};

    for (const PropertyTypeID id : _decided) {
        _isDecided[id.getValue()] = false;
    }
    _decided.clear();

    for (const auto& part : rv::reverse(_view->dataparts())) {
        const PropertyManager& properties = partProperties<IDT>(*part);
        const IDT firstEntity = firstPartEntity<IDT>(*part);

        for (const auto& [id, container] : properties) {
            const size_t index = id.getValue();
            if (index >= _isDecided.size()) {
                _isDecided.resize(index + 1, false);
            }

            if (!container->hasEntry(entityID) || _isDecided[index]) {
                continue;
            }

            _isDecided[index] = true;
            _decided.push_back(id);

            if (container->has(entityID)) {
                add(id, _readsValues ? storedValue(*container, entityID) : PropertyNull {});
            }
        }

        if (entity >= firstEntity) {
            return;
        }
    }
}

template <TypedInternalID IDT>
void NLEntityProperties::collectPending(IDT entity) {
    for (const CommitWriteBuffer::UntypedProperty& property : getPendingProperties(entity)) {
        addWritten(property.propertyID, property.value);
    }
}

template <TypedInternalID IDT>
void NLEntityProperties::collectUpdates(IDT entity) {
    if (!_writeBuffer) {
        return;
    }

    _written->indexUpdates(_writeBuffer);
    if (!_written->hasUpdates()) {
        return;
    }

    const std::span<const PropertyTypeID> updated = _written->findUpdatedProperties(entity);

    for (const PropertyTypeID id : updated) {
        std::erase_if(_properties, [id](const Property& property) { return property._id == id; });
        addWritten(id, *_written->findUpdate(entity, id));
    }
}

template <TypedInternalID IDT>
bool NLEntityProperties::readCommittedValue(IDT entity, PropertyTypeID id, MapBuffer<>::MapItemVariant& value) const {
    const EntityID entityID {entity.getValue()};

    for (const auto& part : rv::reverse(_view->dataparts())) {
        const PropertyManager& properties = partProperties<IDT>(*part);
        const auto found = properties.find(id);

        const PropertyContainer* container = found == properties.end() ? nullptr : found->second.get();
        const bool holdsAnEntry = container && container->hasEntry(entityID);

        if (holdsAnEntry && !container->has(entityID)) {
            return false;
        } else if (holdsAnEntry) {
            value = storedValue(*container, entityID);
            return true;
        } else if (entity >= firstPartEntity<IDT>(*part)) {
            return false;
        }
    }

    return false;
}

template <TypedInternalID IDT>
bool NLEntityProperties::readPendingValue(IDT entity, const PropertyType& type, MapBuffer<>::MapItemVariant& value) {
    const CommitWriteBuffer::UntypedProperties& properties = getPendingProperties(entity);

    const auto found = std::ranges::find(properties, type._id, &CommitWriteBuffer::UntypedProperty::propertyID);
    if (found == properties.end()) {
        return false;
    }

    return readWrittenValue(type, found->value, value);
}

template <TypedInternalID IDT>
const CommitWriteBuffer::SupportedTypeVariant* NLEntityProperties::findUpdate(IDT entity, PropertyTypeID id) {
    if (!_writeBuffer) {
        return nullptr;
    }

    _written->indexUpdates(_writeBuffer);

    return _written->findUpdate(entity, id);
}

bool NLEntityProperties::readWrittenValue(const PropertyType& type,
                                          const CommitWriteBuffer::SupportedTypeVariant& written,
                                          MapBuffer<>::MapItemVariant& value) {
    if (!holdsValue(written)) {
        return false;
    }

    value = writtenValue(*_written, *_readValues, type._valueType, written);
    return true;
}

template <TypedInternalID IDT>
const CommitWriteBuffer::UntypedProperties& NLEntityProperties::getPendingProperties(IDT entity) const {
    if constexpr (std::is_same_v<IDT, NodeID>) {
        return _writeBuffer->getPendingNode(entity.getValue() - _committedNodeCount).properties;
    } else {
        return _writeBuffer->getPendingEdge(entity.getValue() - _committedEdgeCount).properties;
    }
}

template <TypedInternalID IDT>
bool NLEntityProperties::isPending(IDT entity) const {
    if (!_writeBuffer) {
        return false;
    }

    const uint64_t id = entity.getValue();

    if constexpr (std::is_same_v<IDT, NodeID>) {
        return id >= _committedNodeCount && id - _committedNodeCount < _writeBuffer->numPendingNodes();
    } else {
        return id >= _committedEdgeCount && id - _committedEdgeCount < _writeBuffer->numPendingEdges();
    }
}

void NLEntityProperties::addWritten(PropertyTypeID id, const CommitWriteBuffer::SupportedTypeVariant& value) {
    if (!holdsValue(value)) {
        return;
    }

    add(id, _readsValues ? writtenValue(*_written, *_readValues, getValueType(id), value) : PropertyNull {});
}

void NLEntityProperties::add(PropertyTypeID id, const MapBuffer<>::MapItemVariant& value) {
    _properties.push_back(Property {._id = id, ._name = getName(id), ._value = value});
}

PropertyType NLEntityProperties::findPropertyType(std::string_view name) {
    const bool isResolved = _resolvedType.isValid() && name == _resolvedName;
    if (isResolved) {
        return _resolvedType;
    }

    const std::optional<PropertyType> type = findWrittenPropertyType(_view, _metadataBuilder, name);
    if (type) {
        _resolvedName = name;
        _resolvedType = *type;
    }

    return type.value_or(PropertyType {});
}

std::string_view NLEntityProperties::getName(PropertyTypeID id) const {
    const PropertyTypeMap& propertyTypes = _view->metadata().propTypes();

    const std::optional<std::string_view> name = propertyTypes.getName(id);
    if (name) {
        return *name;
    }

    bioassert(_metadataBuilder, "Property type {} is not in the graph's metadata", id.getValue());

    const std::optional<std::string_view> written = _metadataBuilder->findPropertyTypeName(id);
    bioassert(written, "Property type {} is not in the change's metadata", id.getValue());

    return *written;
}

ValueType NLEntityProperties::getValueType(PropertyTypeID id) const {
    const std::optional<PropertyType> type = findWrittenPropertyType(_view, _metadataBuilder, id);
    bioassert(type, "Property type {} is in neither the graph's nor the change's metadata", id.getValue());

    return type->_valueType;
}

template void NLEntityProperties::readKeys<NodeID>(NodeID entity, std::vector<std::string_view>& keys);
template void NLEntityProperties::readKeys<EdgeID>(EdgeID entity, std::vector<std::string_view>& keys);
template void NLEntityProperties::readIDs<NodeID>(NodeID entity, std::vector<PropertyTypeID>& ids);
template void NLEntityProperties::readIDs<EdgeID>(EdgeID entity, std::vector<PropertyTypeID>& ids);
template void NLEntityProperties::readEntries<NodeID>(NodeID entity, std::vector<Entry>& entries);
template void NLEntityProperties::readEntries<EdgeID>(EdgeID entity, std::vector<Entry>& entries);
template bool NLEntityProperties::readValue<NodeID>(NodeID entity, std::string_view name, MapBuffer<>::MapItemVariant& value);
template bool NLEntityProperties::readValue<EdgeID>(EdgeID entity, std::string_view name, MapBuffer<>::MapItemVariant& value);
