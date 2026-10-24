#include "NLPropertyFunctions.h"

#include "list/ListBufferTypeTag.h"
#include "map/MapBufferTypeTag.h"

#include "NLExecutionContext.h"
#include "NLStepBuffers.h"

using namespace db;

namespace {

ListBufferTypeTag cellTag(const ListElementView cell) {
    return cell.getTag();
}

MapBufferTypeTag cellTag(const MapEntryView entry) {
    return entry.getValueTag();
}

template <typename T>
T cellValue(const ListElementView cell) {
    return cell.getAs<T>();
}

template <typename T>
T cellValue(const MapEntryView entry) {
    return entry.getValueAs<T>();
}

}

template <TypedInternalID IDT>
NLKeysFunction<IDT>::NLKeysFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _properties(context, &memory->valueStore()),
    _listBuffer(&memory->listBuffer())
{
}

template <TypedInternalID IDT>
NLKeysFunction<IDT>::~NLKeysFunction() {
}

template <TypedInternalID IDT>
ListView NLKeysFunction<IDT>::operator()(IDT entity) {
    _properties.readKeys(entity, _keys);

    _elements.clear();
    for (const std::string_view key : _keys) {
        _elements.emplace_back(key);
    }

    return _listBuffer->insert(_elements);
}

template <TypedInternalID IDT>
NLPropertiesFunction<IDT>::NLPropertiesFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _properties(context, &memory->valueStore()),
    _mapBuffer(&memory->mapBuffer())
{
}

template <TypedInternalID IDT>
NLPropertiesFunction<IDT>::~NLPropertiesFunction() {
}

template <TypedInternalID IDT>
MapView NLPropertiesFunction<IDT>::operator()(IDT entity) {
    _properties.readEntries(entity, _entries);

    return _mapBuffer->insert(_entries);
}

template <typename Cell>
NLTaggedKeysFunction<Cell>::NLTaggedKeysFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _nodeKeys(context, memory),
    _edgeKeys(context, memory),
    _mapKeys(&memory->listBuffer())
{
}

template <typename Cell>
NLTaggedKeysFunction<Cell>::~NLTaggedKeysFunction() {
}

template <typename Cell>
std::optional<ListView> NLTaggedKeysFunction<Cell>::operator()(const Cell cell) {
    using Tag = decltype(cellTag(cell));
    const Tag tag = cellTag(cell);

    if (tag == Tag::NodeID) {
        return _nodeKeys(cellValue<NodeID>(cell));
    } else if (tag == Tag::EdgeID) {
        return _edgeKeys(cellValue<EdgeID>(cell));
    } else if (tag == Tag::MapView) {
        return _mapKeys(cellValue<MapView>(cell));
    } else if (tag == Tag::Null) {
        return std::nullopt;
    }

    throwCellTypeError("keys", "a node, a relationship or a map");
}

template <typename Cell>
std::optional<ListView> NLTaggedKeysFunction<Cell>::operator()(const std::optional<Cell>& cell) {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

template <typename Cell>
NLTaggedLabelsFunction<Cell>::NLTaggedLabelsFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _labels(*context->getView(), &memory->listBuffer(), context->getWriteBuffer())
{
}

template <typename Cell>
NLTaggedLabelsFunction<Cell>::~NLTaggedLabelsFunction() {
}

template <typename Cell>
std::optional<ListView> NLTaggedLabelsFunction<Cell>::operator()(const Cell cell) {
    using Tag = decltype(cellTag(cell));
    const Tag tag = cellTag(cell);

    if (tag == Tag::NodeID) {
        return _labels(cellValue<NodeID>(cell));
    } else if (tag == Tag::Null) {
        return std::nullopt;
    }

    throwCellTypeError("labels", "a node");
}

template <typename Cell>
std::optional<ListView> NLTaggedLabelsFunction<Cell>::operator()(const std::optional<Cell>& cell) {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

template <typename Cell>
NLTaggedEdgeTypeFunction<Cell>::NLTaggedEdgeTypeFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _edgeType(*context->getView(), &memory->stringBuffer(), context->getWriteBuffer())
{
}

template <typename Cell>
NLTaggedEdgeTypeFunction<Cell>::~NLTaggedEdgeTypeFunction() {
}

template <typename Cell>
std::optional<types::String::Primitive> NLTaggedEdgeTypeFunction<Cell>::operator()(const Cell cell) {
    using Tag = decltype(cellTag(cell));
    const Tag tag = cellTag(cell);

    if (tag == Tag::EdgeID) {
        return _edgeType(cellValue<EdgeID>(cell));
    } else if (tag == Tag::Null) {
        return std::nullopt;
    }

    throwCellTypeError("type", "a relationship");
}

template <typename Cell>
std::optional<types::String::Primitive> NLTaggedEdgeTypeFunction<Cell>::operator()(const std::optional<Cell>& cell) {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

template <typename Cell>
NLTaggedPropertiesFunction<Cell>::NLTaggedPropertiesFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _nodeProperties(context, memory),
    _edgeProperties(context, memory)
{
}

template <typename Cell>
NLTaggedPropertiesFunction<Cell>::~NLTaggedPropertiesFunction() {
}

template <typename Cell>
std::optional<MapView> NLTaggedPropertiesFunction<Cell>::operator()(const Cell cell) {
    using Tag = decltype(cellTag(cell));
    const Tag tag = cellTag(cell);

    if (tag == Tag::NodeID) {
        return _nodeProperties(cellValue<NodeID>(cell));
    } else if (tag == Tag::EdgeID) {
        return _edgeProperties(cellValue<EdgeID>(cell));
    } else if (tag == Tag::MapView) {
        return cellValue<MapView>(cell);
    } else if (tag == Tag::Null) {
        return std::nullopt;
    }

    throwCellTypeError("properties", "a node, a relationship or a map");
}

template <typename Cell>
std::optional<MapView> NLTaggedPropertiesFunction<Cell>::operator()(const std::optional<Cell>& cell) {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

namespace db {

template class NLKeysFunction<NodeID>;
template class NLKeysFunction<EdgeID>;
template class NLPropertiesFunction<NodeID>;
template class NLPropertiesFunction<EdgeID>;
template class NLTaggedKeysFunction<ListElementView>;
template class NLTaggedKeysFunction<MapEntryView>;
template class NLTaggedLabelsFunction<ListElementView>;
template class NLTaggedLabelsFunction<MapEntryView>;
template class NLTaggedEdgeTypeFunction<ListElementView>;
template class NLTaggedEdgeTypeFunction<MapEntryView>;
template class NLTaggedPropertiesFunction<ListElementView>;
template class NLTaggedPropertiesFunction<MapEntryView>;

}
