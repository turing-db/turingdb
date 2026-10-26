#include "NLPropertyFunctions.h"

#include "CellTag.h"

#include "NLExecutionContext.h"
#include "NLStepBuffers.h"

using namespace db;

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

NLTaggedKeysFunction::NLTaggedKeysFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _nodeKeys(context, memory),
    _edgeKeys(context, memory),
    _mapKeys(&memory->listBuffer())
{
}

NLTaggedKeysFunction::~NLTaggedKeysFunction() {
}

std::optional<ListView> NLTaggedKeysFunction::operator()(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::NodeID) {
        return _nodeKeys(cell.getAs<NodeID>());
    } else if (tag == CellTag::EdgeID) {
        return _edgeKeys(cell.getAs<EdgeID>());
    } else if (tag == CellTag::MapView) {
        return _mapKeys(cell.getAs<MapView>());
    } else if (tag == CellTag::Null) {
        return std::nullopt;
    }

    throwCellTypeError("keys", "a node, a relationship or a map");
}

std::optional<ListView> NLTaggedKeysFunction::operator()(const std::optional<CellView>& cell) {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

NLTaggedLabelsFunction::NLTaggedLabelsFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _labels(*context->getView(), &memory->listBuffer(), context->getWriteBuffer())
{
}

NLTaggedLabelsFunction::~NLTaggedLabelsFunction() {
}

std::optional<ListView> NLTaggedLabelsFunction::operator()(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::NodeID) {
        return _labels(cell.getAs<NodeID>());
    } else if (tag == CellTag::Null) {
        return std::nullopt;
    }

    throwCellTypeError("labels", "a node");
}

std::optional<ListView> NLTaggedLabelsFunction::operator()(const std::optional<CellView>& cell) {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

NLTaggedEdgeTypeFunction::NLTaggedEdgeTypeFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _edgeType(*context->getView(), &memory->stringBuffer(), context->getWriteBuffer())
{
}

NLTaggedEdgeTypeFunction::~NLTaggedEdgeTypeFunction() {
}

std::optional<types::String::Primitive> NLTaggedEdgeTypeFunction::operator()(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::EdgeID) {
        return _edgeType(cell.getAs<EdgeID>());
    } else if (tag == CellTag::Null) {
        return std::nullopt;
    }

    throwCellTypeError("type", "a relationship");
}

std::optional<types::String::Primitive> NLTaggedEdgeTypeFunction::operator()(const std::optional<CellView>& cell) {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

NLTaggedPropertiesFunction::NLTaggedPropertiesFunction(NLExecutionContext* context, NLStepBuffers* memory)
    : _nodeProperties(context, memory),
    _edgeProperties(context, memory)
{
}

NLTaggedPropertiesFunction::~NLTaggedPropertiesFunction() {
}

std::optional<MapView> NLTaggedPropertiesFunction::operator()(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::NodeID) {
        return _nodeProperties(cell.getAs<NodeID>());
    } else if (tag == CellTag::EdgeID) {
        return _edgeProperties(cell.getAs<EdgeID>());
    } else if (tag == CellTag::MapView) {
        return cell.getAs<MapView>();
    } else if (tag == CellTag::Null) {
        return std::nullopt;
    }

    throwCellTypeError("properties", "a node, a relationship or a map");
}

std::optional<MapView> NLTaggedPropertiesFunction::operator()(const std::optional<CellView>& cell) {
    return cell.has_value() ? (*this)(*cell) : std::nullopt;
}

namespace db {

template class NLKeysFunction<NodeID>;
template class NLKeysFunction<EdgeID>;
template class NLPropertiesFunction<NodeID>;
template class NLPropertiesFunction<EdgeID>;

}
