#include "NLExecutor.h"

#include <stdint.h>
#include <span>
#include <string_view>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "TypeUtils.h"
#include "columns/ContainerKind.h"
#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "columns/IndexTypeError.h"
#include "CellUtils.h"
#include "QueryTypeName.h"
#include "map/MapUtils.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLEntityProperties.h"
#include "NLColumnKernels.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

template <typename Primitive, typename ResCol, typename LhsCol, typename RhsCol>
void applyValueListIndex(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* memory) {
    BinaryOperators::exec<ValueListIndex<Primitive>>(static_cast<ResCol*>(result),
                                                     static_cast<const LhsCol*>(lhs),
                                                     static_cast<const RhsCol*>(rhs));
}

template <typename Primitive>
struct ValueListIndexSelector {
    LocalMemory* _memory {nullptr};
    Column* _result {nullptr};
    NLBinaryFn _fn {nullptr};

    template <typename LhsCol, typename RhsCol>
    void operator()(const LhsCol*, const RhsCol*) {
        using ResCol = ColumnCombination<ValueListIndex<Primitive>, LhsCol, RhsCol>;
        using ResColType = ResCol::ResultColumnType;

        _result = _memory->alloc<ResColType>();
        _fn = &applyValueListIndex<Primitive, ResColType, LhsCol, RhsCol>;
    }
};

// The entity sibling of ValueListIndex: an ID column spells a null entity as an invalid
// ID, so a position past the end - or the tagged null a list holds for an entity an
// OPTIONAL MATCH did not match - reads back as one rather than as an absent optional.
template <typename IDType>
struct EntityListIndex {
    template <typename List, typename Index>
    IDType operator()(const List& list, const Index& index) const {
        return ValueListIndex<IDType> {}(list, index).value_or(IDType {});
    }
};

template <typename IDType, typename ResCol, typename LhsCol, typename RhsCol>
void applyEntityListIndex(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers*) {
    BinaryOpExecutor<EntityListIndex<IDType>>::apply(static_cast<ResCol*>(result),
                                                     static_cast<const LhsCol*>(lhs),
                                                     static_cast<const RhsCol*>(rhs));
}

template <typename IDType>
struct EntityListIndexSelector {
    LocalMemory* _memory {nullptr};
    Column* _result {nullptr};
    NLBinaryFn _fn {nullptr};

    template <typename LhsCol, typename RhsCol>
    void operator()(const LhsCol*, const RhsCol*) {
        using Lists = typename InnerTypeHelper<LhsCol>::type;
        using Indices = typename InnerTypeHelper<RhsCol>::type;

        constexpr bool readsAList = std::is_same_v<TypeUtils::unwrap_optional_t<Lists>, ListView>;
        constexpr bool readsConstants = std::is_same_v<LhsCol, ColumnConst<Lists>>
                                     && std::is_same_v<RhsCol, ColumnConst<Indices>>;
        using ResCol = std::conditional_t<readsConstants, ColumnConst<IDType>, ColumnVector<IDType>>;

        if constexpr (readsAList) {
            _result = _memory->alloc<ResCol>();
            _fn = &applyEntityListIndex<IDType, ResCol, LhsCol, RhsCol>;
        }
    }
};

template <typename IDType>
NLBinaryFn selectEntityListIndexOf(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result) {
    using Pairs = PairRestrictions<OP_INDEX>;

    EntityListIndexSelector<IDType> selector {._memory = memory};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           EntityListIndexSelector<IDType>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, selector);

    result = selector._result;
    return selector._fn;
}

// The map sibling of the list reads above: one cell of a map column, or nothing where the
// row holds no map.
template <typename ColumnT>
NLKeyedValue mapRead(const Column* input, size_t row) {
    const std::optional<MapView> map = operandRow(static_cast<const ColumnT*>(input), row);
    if (!map) {
        return std::monostate {};
    }

    return *map;
}

NLKeyedValue readKeyedCell(const CellView cell) {
    const CellTag tag = cell.getTag();

    switch (tag) {
        case CellTag::MapView:
            return cell.getAs<MapView>();
        break;
        case CellTag::NodeID:
            return cell.getAs<NodeID>();
        break;
        case CellTag::EdgeID:
            return cell.getAs<EdgeID>();
        break;
        case CellTag::DateTime:
            return cell.getAs<DateTime>();
        break;
        case CellTag::Duration:
            return cell.getAs<Duration>();
        break;
        case CellTag::Point:
            return cell.getAs<Point>();
        break;
        case CellTag::Null:
            return std::monostate {};
        break;
        case CellTag::Int:
        case CellTag::UInt:
        case CellTag::Double:
        case CellTag::Bool:
        case CellTag::String:
        case CellTag::Embedding:
        case CellTag::ListView:
        case CellTag::Path:
        case CellTag::LabelID:
        case CellTag::EdgeTypeID:
        case CellTag::PropertyTypeID:
        case CellTag::ValueType:
            return NLKeylessValue {tag};
        break;
        case CellTag::INVALID:
            throw IRException("a map key read reads an untagged cell");
        break;
    }

    throw IRException("Unknown CellTag");
}

// One cell of a column of tagged cells, read as what its own tag says it holds: a map, a
// node or an edge read out of a list or out of another map arrives as such a column
template <typename ColumnT>
NLKeyedValue readTaggedKeyed(const Column* input, size_t row) {
    return readKeyedCell((*static_cast<const ColumnT*>(input))[row]);
}

template <typename Held>
CellTag heldTag(const Held& held) {
    const auto tagOf = []<typename Value>(const Value& value) {
        if constexpr (std::is_same_v<Value, std::monostate>) {
            return CellTag::Null;
        } else if constexpr (std::is_same_v<Value, NLKeylessValue> || std::is_same_v<Value, NLNeitherPositionNorKey>) {
            return value._tag;
        } else {
            return TypeToCellTag<Value>::Tag;
        }
    };

    return std::visit(tagOf, held);
}

// The entry @param keyed holds under @param key: the one its map holds, or its node's or
// edge's property, written into @param mapBuffer as a map of that one entry. False where
// it holds null or no such entry, and raises where it holds a value with no entries.
bool findKeyedEntry(const NLKeyedValue& keyed,
                    std::string_view key,
                    NLEntityProperties& properties,
                    MapBuffer<>& mapBuffer,
                    MapEntryView& entry) {
    MapBuffer<>::MapKeyValuePair property {.key = key};

    if (std::holds_alternative<std::monostate>(keyed)) {
        return false;
    } else if (const MapView* map = std::get_if<MapView>(&keyed)) {
        return findMapEntry(*map, key, entry);
    } else if (const NodeID* node = std::get_if<NodeID>(&keyed)) {
        if (!properties.readValue(*node, key, property.value)) {
            return false;
        }
    } else if (const EdgeID* edge = std::get_if<EdgeID>(&keyed)) {
        if (!properties.readValue(*edge, key, property.value)) {
            return false;
        }
    } else {
        throwIndexTypeError(heldTag(keyed), CellTag::String);
    }

    entry = mapBuffer.insert(std::span<const MapBuffer<>::MapKeyValuePair> {&property, 1}).front();
    return true;
}

MapBuffer<>::MapItemVariant mapItemOf(int64_t component) {
    return component;
}

MapBuffer<>::MapItemVariant mapItemOf(const Point::Component& component) {
    return std::visit([](auto value) -> MapBuffer<>::MapItemVariant { return value; }, component);
}

// The component @param part names of @param value, written into @param mapBuffer as the
// value of a map of one entry, as an entity's property is
template <typename Owner, typename Part>
CellView componentCell(Owner value,
                       std::optional<Part> part,
                       std::string_view key,
                       std::string_view ownerName,
                       MapBuffer<>& mapBuffer) {
    if (!part) {
        throw IRException(fmt::format("'{}' is not a component of a {}", key, ownerName));
    }

    const MapBuffer<>::MapKeyValuePair component {
        .key = key,
        .value = mapItemOf(Owner::component(value, *part)),
    };

    const MapView map = mapBuffer.insert(std::span<const MapBuffer<>::MapKeyValuePair> {&component, 1});

    return map.front().getValue();
}

// The value a property read finds in @param keyed: a key of its map, a property of its node
// or edge, or a component of its instant, duration or point. Null where it holds null or no
// such key.
CellView readStaticKey(const NLKeyedValue& keyed,
                       const NLStaticMapKeyData* mapKey,
                       NLEntityProperties& properties,
                       MapBuffer<>& mapBuffer) {
    const std::string_view key = mapKey->getKey();

    if (const DateTime* instant = std::get_if<DateTime>(&keyed)) {
        return componentCell(*instant, mapKey->getPart<DateTimePart>(), key, "datetime", mapBuffer);
    } else if (const Duration* duration = std::get_if<Duration>(&keyed)) {
        return componentCell(*duration, mapKey->getPart<DurationPart>(), key, "duration", mapBuffer);
    } else if (const Point* point = std::get_if<Point>(&keyed)) {
        return componentCell(*point, mapKey->getPart<PointPart>(), key, "point", mapBuffer);
    } else if (const NLKeylessValue* keyless = std::get_if<NLKeylessValue>(&keyed)) {
        throw IRException(fmt::format("Type mismatch: a value of type '{}' has no property '{}'",
                                      queryTypeName(keyless->_tag),
                                      key));
    }

    MapEntryView entry;
    if (!findKeyedEntry(keyed, key, properties, mapBuffer, entry)) {
        return CellView::nullElement();
    }

    return entry.getValue();
}

template <typename ColumnT>
NLContainer listContainer(const Column* input, size_t row) {
    const std::optional<ListView> list = operandRow(static_cast<const ColumnT*>(input), row);
    if (!list) {
        return NLContainer {};
    }

    return NLContainer {._list = list, ._keyed = NLKeylessValue {CellTag::ListView}};
}

template <typename ColumnT>
NLContainer mapContainer(const Column* input, size_t row) {
    return NLContainer {._keyed = mapRead<ColumnT>(input, row)};
}

template <typename ColumnT>
NLContainer cellContainer(const Column* input, size_t row) {
    const CellView cell = (*static_cast<const ColumnT*>(input))[row];
    if (!holdsAList(cell)) {
        return NLContainer {._keyed = readKeyedCell(cell)};
    }

    return NLContainer {._list = operandList(cell), ._keyed = NLKeylessValue {CellTag::ListView}};
}

NLSubscript subscriptOf(const CellView cell) {
    const CellTag tag = cell.getTag();

    switch (tag) {
        case CellTag::Int:
            return cell.getAs<types::Int64::Primitive>();
        break;
        case CellTag::UInt:
            return cell.getAs<types::UInt64::Primitive>();
        break;
        case CellTag::String:
            return cell.getAs<types::String::Primitive>();
        break;
        case CellTag::Null:
            return std::monostate {};
        break;
        case CellTag::Double:
        case CellTag::Bool:
        case CellTag::Embedding:
        case CellTag::ListView:
        case CellTag::NodeID:
        case CellTag::EdgeID:
        case CellTag::DateTime:
        case CellTag::MapView:
        case CellTag::Duration:
        case CellTag::Path:
        case CellTag::Point:
        case CellTag::LabelID:
        case CellTag::EdgeTypeID:
        case CellTag::PropertyTypeID:
        case CellTag::ValueType:
            return NLNeitherPositionNorKey {tag};
        break;
        case CellTag::INVALID:
            throw IRException("an index reads an untagged cell");
        break;
    }

    throw IRException("Unknown CellTag");
}

template <typename ColumnT>
NLSubscript readSubscript(const Column* input, size_t row) {
    return subscriptOf((*static_cast<const ColumnT*>(input))[row]);
}

// A constant stands for every row of the other operand, which a WHERE may have left empty
size_t pairedRowCount(const Column* lhs, const Column* rhs) {
    const bool lhsIsConstant = lhs->getContainerKind() == ContainerKind::code<ColumnConst>();
    const bool rhsIsConstant = rhs->getContainerKind() == ContainerKind::code<ColumnConst>();

    if (lhsIsConstant && rhsIsConstant) {
        return 1;
    } else if (lhsIsConstant) {
        return rhs->size();
    } else if (rhsIsConstant) {
        return lhs->size();
    }

    bioassert(lhs->size() == rhs->size(), "The two operands of a row-wise read are not row-aligned.");

    return lhs->size();
}

CellView readContainerElement(const NLContainer& container,
                              const NLSubscript& subscript,
                              NLEntityProperties& properties,
                              MapBuffer<>& mapBuffer) {
    const bool readsANull = std::holds_alternative<std::monostate>(subscript)
                         || std::holds_alternative<std::monostate>(container._keyed);
    const bool readsAPositionOfAList = container._list.has_value()
                                    && !std::holds_alternative<NLNeitherPositionNorKey>(subscript);

    if (readsANull) {
        return CellView::nullElement();
    } else if (const types::String::Primitive* key = std::get_if<types::String::Primitive>(&subscript)) {
        MapEntryView entry;
        if (!findKeyedEntry(container._keyed, *key, properties, mapBuffer, entry)) {
            return CellView::nullElement();
        }

        return entry.getValue();
    } else if (!readsAPositionOfAList) {
        throwIndexTypeError(heldTag(container._keyed), heldTag(subscript));
    } else if (const types::Int64::Primitive* position = std::get_if<types::Int64::Primitive>(&subscript)) {
        return ListIndexImpl {}(*container._list, *position);
    }

    return ListIndexImpl {}(*container._list, std::get<types::UInt64::Primitive>(subscript));
}

template <typename Element>
Element containerIndexResult(CellView element) {
    if constexpr (std::is_same_v<Element, CellView>) {
        return element;
    } else if constexpr (TypeUtils::is_optional_v<Element>) {
        return ValueListIndexImpl<typename Element::value_type>::elementValue(element);
    } else {
        return ValueListIndexImpl<Element>::elementValue(element).value_or(Element {});
    }
}

}

NLKeyedReadFunction NLExecutor::selectKeyedRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<MapView>::staticKind()) {
        return &mapRead<ColumnVector<MapView>>;
    } else if (kind == ColumnOptVector<MapView>::staticKind()) {
        return &mapRead<ColumnOptVector<MapView>>;
    } else if (kind == ColumnConst<MapView>::staticKind()) {
        return &mapRead<ColumnConst<MapView>>;
    } else if (kind == ColumnConst<std::optional<MapView>>::staticKind()) {
        return &mapRead<ColumnConst<std::optional<MapView>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &readTaggedKeyed<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &readTaggedKeyed<ColumnConst<CellView>>;
    }

    throw IRException("a map key read reads a map column, or a column of tagged cells");
}

// Row r of the result views the value row r's map already holds under the key, so the read
// copies nothing. A null row, and a map holding no such key, read back as a null cell.
void NLExecutor::runStaticMapKey(NLExecutionContext* context, NLFunctionData* data) {
    const NLStaticMapKeyData* mapKey = static_cast<NLStaticMapKeyData*>(data);

    const Column* input = mapKey->getInput();
    const NLKeyedReadFunction read = mapKey->getKeyedRead();

    NLStepBuffers* const stepBuffers = mapKey->getStepBuffers();
    stepBuffers->clear();

    MapBuffer<>& mapBuffer = stepBuffers->mapBuffer();
    NLEntityProperties properties(context, &stepBuffers->valueStore());

    const auto valueAt = [input, read, mapKey, &properties, &mapBuffer](size_t row) {
        return readStaticKey(read(input, row), mapKey, properties, mapBuffer);
    };

    Column* result = mapKey->getResult();

    if (result->getKind() == ColumnConst<CellView>::staticKind()) {
        static_cast<ColumnConst<CellView>*>(result)->set(valueAt(0));
        return;
    }

    std::vector<CellView>& outputRaw = static_cast<ColumnVector<CellView>*>(result)->getRaw();
    const size_t rowCount = input->size();

    outputRaw.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        outputRaw[row] = valueAt(row);
    }
}

// Row r of the result views the value row r's map already holds under row r's key, so the
// read copies nothing. A map holding no such key, and a row holding no key, read back as a
// null cell.
void NLExecutor::runDynamicMapKey(NLExecutionContext* context, NLFunctionData* data) {
    const NLDynamicMapKeyData* dynamicMapKey = static_cast<NLDynamicMapKeyData*>(data);

    const Column* input = dynamicMapKey->getInput();
    const Column* keyColumn = dynamicMapKey->getKey();

    const NLKeyedReadFunction readKeyed = dynamicMapKey->getKeyedRead();
    const NLStringReadFunction readKey = dynamicMapKey->getKeyRead();

    NLStepBuffers* const stepBuffers = dynamicMapKey->getStepBuffers();
    stepBuffers->clear();

    MapBuffer<>& mapBuffer = stepBuffers->mapBuffer();
    NLEntityProperties properties(context, &stepBuffers->valueStore());

    const auto valueAt = [input, keyColumn, readKeyed, readKey, &properties, &mapBuffer](size_t row) {
        const std::optional<types::String::Primitive> key = readKey(keyColumn, row);

        MapEntryView entry;
        if (!key || !findKeyedEntry(readKeyed(input, row), *key, properties, mapBuffer, entry)) {
            return CellView::nullElement();
        }

        return entry.getValue();
    };

    Column* result = dynamicMapKey->getResult();

    if (result->getKind() == ColumnConst<CellView>::staticKind()) {
        static_cast<ColumnConst<CellView>*>(result)->set(valueAt(0));
        return;
    }

    const size_t rowCount = pairedRowCount(input, keyColumn);

    std::vector<CellView>& outputRaw = static_cast<ColumnVector<CellView>*>(result)->getRaw();

    outputRaw.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        outputRaw[row] = valueAt(row);
    }
}

template <typename Element>
void NLExecutor::runContainerIndex(NLExecutionContext* context, NLFunctionData* data) {
    const NLContainerIndexData* containerIndex = static_cast<NLContainerIndexData*>(data);

    const Column* base = containerIndex->getBase();
    const Column* index = containerIndex->getIndex();

    const NLContainerReadFunction readBase = containerIndex->getBaseRead();
    const NLSubscriptReadFunction readIndex = containerIndex->getIndexRead();

    NLStepBuffers* const stepBuffers = containerIndex->getStepBuffers();
    stepBuffers->clear();

    MapBuffer<>& mapBuffer = stepBuffers->mapBuffer();
    NLEntityProperties properties(context, &stepBuffers->valueStore());

    const size_t rowCount = pairedRowCount(base, index);

    using OutputColumn = ColumnVectorOf<Element>;
    OutputColumn* output = static_cast<OutputColumn*>(containerIndex->getResult());

    resetForRowWrites(output, rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        const CellView element = readContainerElement(readBase(base, row),
                                                      readIndex(index, row),
                                                      properties,
                                                      mapBuffer);

        writeRow(output, row, containerIndexResult<Element>(element));
    }
}

NLHandlerFunction NLExecutor::selectCellContainerIndex() {
    return &runContainerIndex<CellView>;
}

NLHandlerFunction NLExecutor::selectEntityContainerIndex(NLChunkKind kind) {
    if (kind == NLChunkKind::NodeID) {
        return &runContainerIndex<NodeID>;
    } else if (kind == NLChunkKind::EdgeID) {
        return &runContainerIndex<EdgeID>;
    }

    throw IRException("a container index reads its entities out of a list of nodes or of edges");
}

NLHandlerFunction NLExecutor::selectValueContainerIndex(NLChunkKind kind) {
    NLHandlerFunction selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &runContainerIndex<std::optional<Element>>; });

    return selected;
}

NLContainerReadFunction NLExecutor::selectContainerRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<ListView>::staticKind()) {
        return &listContainer<ColumnVector<ListView>>;
    } else if (kind == ColumnOptVector<ListView>::staticKind()) {
        return &listContainer<ColumnOptVector<ListView>>;
    } else if (kind == ColumnConst<ListView>::staticKind()) {
        return &listContainer<ColumnConst<ListView>>;
    } else if (kind == ColumnConst<std::optional<ListView>>::staticKind()) {
        return &listContainer<ColumnConst<std::optional<ListView>>>;
    } else if (kind == ColumnVector<MapView>::staticKind()) {
        return &mapContainer<ColumnVector<MapView>>;
    } else if (kind == ColumnOptVector<MapView>::staticKind()) {
        return &mapContainer<ColumnOptVector<MapView>>;
    } else if (kind == ColumnConst<MapView>::staticKind()) {
        return &mapContainer<ColumnConst<MapView>>;
    } else if (kind == ColumnConst<std::optional<MapView>>::staticKind()) {
        return &mapContainer<ColumnConst<std::optional<MapView>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellContainer<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &cellContainer<ColumnConst<CellView>>;
    }

    throw IRException("a container index reads a list column, a map column or a column of tagged cells");
}

NLSubscriptReadFunction NLExecutor::selectSubscriptRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<CellView>::staticKind()) {
        return &readSubscript<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &readSubscript<ColumnConst<CellView>>;
    }

    throw IRException("a container index reads its index out of a column of tagged cells");
}

NLBinaryFn NLExecutor::selectEntityListIndex(NLChunkKind kind,
                                             const Column* lhs,
                                             const Column* rhs,
                                             LocalMemory* memory,
                                             Column*& result) {
    NLBinaryFn selected = nullptr;

    if (kind == NLChunkKind::NodeID) {
        selected = selectEntityListIndexOf<NodeID>(lhs, rhs, memory, result);
    } else if (kind == NLChunkKind::EdgeID) {
        selected = selectEntityListIndexOf<EdgeID>(lhs, rhs, memory, result);
    } else {
        throw IRException("Only a node or an edge column can hold an entity read out of a list");
    }

    if (!selected) {
        throw IRException("An entity is read out of a list column, not out of a type-erased one");
    }

    return selected;
}

NLBinaryFn NLExecutor::selectValueListIndex(NLChunkKind kind,
                                            const Column* lhs,
                                            const Column* rhs,
                                            LocalMemory* memory,
                                            Column*& result) {
    using Pairs = PairRestrictions<OP_INDEX>;

    NLBinaryFn selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        using Selector = ValueListIndexSelector<Element>;

        Selector selector {._memory = memory};
        ColumnDoubleDispatcher<typename Pairs::Allowed,
                               Selector,
                               typename Pairs::Excluded>::dispatch(lhs, rhs, selector);

        result = selector._result;
        selected = selector._fn;
    });

    return selected;
}
