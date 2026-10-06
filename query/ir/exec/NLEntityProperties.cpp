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
        case ValueType::Invalid:
        case ValueType::_SIZE:
        break;
    }

    bioassert(false, "Property container of an invalid value type");
    return PropertyNull {};
}

template <SupportedType T>
MapBuffer<>::MapItemVariant readWritten(NLWrittenValues& written, const NLWrittenValues::Value& value) {
    const std::optional<typename T::Primitive> read = written.read<T>(value);
    if (!read) {
        return PropertyNull {};
    }

    return *read;
}

// A written value is held as the type its row's column carried; it reads back as the
// type the schema holds the property as, which is what the commit will store it as
MapBuffer<>::MapItemVariant writtenValue(NLWrittenValues& written,
                                         ValueType valueType,
                                         const NLWrittenValues::Value& value) {
    switch (valueType) {
        case ValueType::Int64:
            return readWritten<types::Int64>(written, value);
        break;
        case ValueType::UInt64:
            return readWritten<types::UInt64>(written, value);
        break;
        case ValueType::Double:
            return readWritten<types::Double>(written, value);
        break;
        case ValueType::String:
            return readWritten<types::String>(written, value);
        break;
        case ValueType::Bool:
            return readWritten<types::Bool>(written, value);
        break;
        case ValueType::Embedding:
            return readWritten<types::Embedding>(written, value);
        break;
        case ValueType::List:
            return readWritten<types::List>(written, value);
        break;
        case ValueType::DateTime:
            return readWritten<types::DateTime>(written, value);
        break;
        case ValueType::Map:
            return readWritten<types::Map>(written, value);
        break;
        case ValueType::Duration:
            return readWritten<types::Duration>(written, value);
        break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
        break;
    }

    bioassert(false, "Written property of an invalid value type");
    return PropertyNull {};
}

bool holdsValue(const CommitWriteBuffer::SupportedTypeVariant& value) {
    return std::visit([](const auto& held) { return held.has_value(); }, value);
}

}

NLEntityProperties::NLEntityProperties(NLExecutionContext* context)
    : _view(context->getView()),
    _writeBuffer(context->getWriteBuffer()),
    _written(&context->getWrittenValues()),
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
void NLEntityProperties::readEntries(IDT entity, std::vector<Entry>& entries) {
    _readsValues = true;
    collect(entity);

    entries.clear();
    for (const Property& property : _properties) {
        entries.push_back(Entry {.key = property._name, .value = property._value});
    }
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

    _decided.clear();

    for (const auto& part : rv::reverse(_view->dataparts())) {
        const PropertyManager& properties = std::is_same_v<IDT, NodeID> ? part->nodeProperties() : part->edgeProperties();

        for (const auto& [id, container] : properties) {
            if (!container->hasEntry(entityID) || std::ranges::contains(_decided, id)) {
                continue;
            }

            _decided.push_back(id);

            if (container->has(entityID)) {
                add(id, _readsValues ? storedValue(*container, entityID) : PropertyNull {});
            }
        }
    }
}

template <TypedInternalID IDT>
void NLEntityProperties::collectPending(IDT entity) {
    if constexpr (std::is_same_v<IDT, NodeID>) {
        const CommitWriteBuffer::PendingNode& node = _writeBuffer->getPendingNode(entity.getValue() - _committedNodeCount);
        for (const CommitWriteBuffer::UntypedProperty& property : node.properties) {
            addWritten(property.propertyID, property.value);
        }
    } else {
        const CommitWriteBuffer::PendingEdge& edge = _writeBuffer->getPendingEdge(entity.getValue() - _committedEdgeCount);
        for (const CommitWriteBuffer::UntypedProperty& property : edge.properties) {
            addWritten(property.propertyID, property.value);
        }
    }
}

template <TypedInternalID IDT>
void NLEntityProperties::collectUpdates(IDT entity) {
    if (!_writeBuffer || !_written->hasUpdates()) {
        return;
    }

    _written->collectUpdatedProperties(entity, _updated);

    for (const PropertyTypeID id : _updated) {
        std::erase_if(_properties, [id](const Property& property) { return property._id == id; });
        addWritten(id, *_written->findUpdate(entity, id));
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

    add(id, _readsValues ? writtenValue(*_written, getValueType(id), value) : PropertyNull {});
}

void NLEntityProperties::add(PropertyTypeID id, const MapBuffer<>::MapItemVariant& value) {
    _properties.push_back(Property {._id = id, ._name = getName(id), ._value = value});
}

std::string_view NLEntityProperties::getName(PropertyTypeID id) const {
    const std::optional<std::string_view> name = _view->metadata().propTypes().getName(id);
    if (name) {
        return *name;
    }

    bioassert(_metadataBuilder, "Property type {} is not in the graph's metadata", id.getValue());

    const std::optional<std::string_view> written = _metadataBuilder->findPropertyTypeName(id);
    bioassert(written, "Property type {} is not in the change's metadata", id.getValue());

    return *written;
}

ValueType NLEntityProperties::getValueType(PropertyTypeID id) const {
    const std::optional<PropertyType> type = _view->metadata().propTypes().get(id);
    if (type) {
        return type->_valueType;
    }

    bioassert(_metadataBuilder, "Property type {} is not in the graph's metadata", id.getValue());

    const std::optional<PropertyType> written = _metadataBuilder->findPropertyType(id);
    bioassert(written, "Property type {} is not in the change's metadata", id.getValue());

    return written->_valueType;
}

template void NLEntityProperties::readKeys<NodeID>(NodeID entity, std::vector<std::string_view>& keys);
template void NLEntityProperties::readKeys<EdgeID>(EdgeID entity, std::vector<std::string_view>& keys);
template void NLEntityProperties::readEntries<NodeID>(NodeID entity, std::vector<Entry>& entries);
template void NLEntityProperties::readEntries<EdgeID>(EdgeID entity, std::vector<Entry>& entries);
