#include "NLWriteProperties.h"

#include <algorithm>
#include <optional>
#include <string_view>
#include <variant>
#include <vector>

#include <spdlog/fmt/bundled/format.h>

#include "columns/AllowedKinds.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnKind.h"
#include "columns/ColumnOperand.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "CellView.h"
#include "CellUtils.h"
#include "list/ListView.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "metadata/GraphMetadata.h"
#include "QueryTypeName.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "BioAssert.h"
#include "IRException.h"

using namespace db;

namespace {

// A path and a schema ID are values of the query that read them, not ones a graph stores: a
// property holding one anywhere inside a list or a map is turned away, as Neo4j turns a path
// away. Empty for a value a property can hold.
std::string_view unstorableValueName(CellTag tag) {
    switch (tag) {
        case CellTag::Path:
            return "a path";
        break;
        case CellTag::LabelID:
            return "a label ID";
        break;
        case CellTag::EdgeTypeID:
            return "an edge type ID";
        break;
        case CellTag::PropertyTypeID:
            return "a property type ID";
        break;
        case CellTag::ValueType:
            return "a value type";
        break;
        case CellTag::Int:
        case CellTag::UInt:
        case CellTag::Double:
        case CellTag::Bool:
        case CellTag::String:
        case CellTag::Embedding:
        case CellTag::ListView:
        case CellTag::Null:
        case CellTag::NodeID:
        case CellTag::EdgeID:
        case CellTag::DateTime:
        case CellTag::MapView:
        case CellTag::Duration:
        case CellTag::Point:
        case CellTag::INVALID:
            return {};
        break;
    }

    return {};
}

void throwIfAnyHoldsAnUnstorableValue(std::vector<CellView>& pending) {
    while (!pending.empty()) {
        const CellView cell = pending.back();
        pending.pop_back();

        const CellTag tag = cell.getTag();
        const std::string_view unstorable = unstorableValueName(tag);

        if (!unstorable.empty()) {
            throw IRException(fmt::format("A property value cannot hold {}, in a list or a map or on its own", unstorable));
        } else if (tag == CellTag::ListView) {
            const ListView list = cell.getAs<ListView>();
            pending.insert(pending.end(), list.begin(), list.end());
        } else if (tag == CellTag::MapView) {
            for (const MapEntryView entry : cell.getAs<MapView>()) {
                pending.push_back(entry.getValue());
            }
        }
    }
}

void throwIfHoldsAnUnstorableValue(ListView list) {
    std::vector<CellView> pending(list.begin(), list.end());
    throwIfAnyHoldsAnUnstorableValue(pending);
}

void throwIfHoldsAnUnstorableValue(MapView map) {
    std::vector<CellView> pending;
    pending.reserve(map.size());

    for (const MapEntryView entry : map) {
        pending.push_back(entry.getValue());
    }

    throwIfAnyHoldsAnUnstorableValue(pending);
}

class ConstPropertyExtractor {
public:
    ConstPropertyExtractor(CommitWriteBuffer::UntypedProperties& buf,
                           PropertyTypeID propID,
                           size_t rowCount)
        : _buf(buf),
        _propID(propID),
        _rowCount(rowCount)
    {
    }

    template <typename T>
    void operator()(const ColumnConst<T>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, std::make_optional(typed->getRaw()));
        }
    }

    void operator()(const ColumnConst<types::String::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, std::make_optional(std::string(typed->getRaw())));
        }
    }

    void operator()(const ColumnConst<types::Embedding::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        const types::Embedding::Primitive span = typed->getRaw();
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, std::make_optional(types::Embedding::OwningPrimitive(span.begin(), span.end())));
        }
    }

    void operator()(const ColumnConst<ListView>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        throwIfHoldsAnUnstorableValue(typed->getRaw());
        const types::List::OwningPrimitive encoded(typed->getRaw());
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, encoded);
        }
    }

    // A conversion of a literal - datetime('...'), toInteger('...') - answers a cell that
    // may hold no value, so the const it stands for is already the optional the write
    // buffer stages and must not be wrapped in a second one.
    template <typename T>
    void operator()(const ColumnConst<std::optional<T>>* typed) {
        fillRows(typed->get(0));
    }

    void operator()(const ColumnConst<std::optional<types::String::Primitive>>* typed) {
        std::optional<types::String::OwningPrimitive> owned;
        if (typed->isValid(0)) {
            owned.emplace(typed->getValue(0));
        }

        fillRows(owned);
    }

    void operator()(const ColumnConst<std::optional<types::Embedding::Primitive>>* typed) {
        std::optional<types::Embedding::OwningPrimitive> owned;
        if (typed->isValid(0)) {
            const types::Embedding::Primitive value = typed->getValue(0);
            owned.emplace(value.begin(), value.end());
        }

        fillRows(owned);
    }

    void operator()(const ColumnConst<std::optional<ListView>>* typed) {
        std::optional<types::List::OwningPrimitive> owned;
        if (typed->isValid(0)) {
            const ListView value = typed->getValue(0);
            throwIfHoldsAnUnstorableValue(value);
            owned.emplace(value);
        }

        fillRows(owned);
    }

    void operator()(const ColumnConst<MapView>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        throwIfHoldsAnUnstorableValue(typed->getRaw());
        const types::Map::OwningPrimitive encoded(typed->getRaw());
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, encoded);
        }
    }

private:
    CommitWriteBuffer::UntypedProperties& _buf;
    PropertyTypeID _propID;
    size_t _rowCount;

    template <typename T>
    void fillRows(const T& value) {
        _buf.clear();
        _buf.reserve(_rowCount);

        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, value);
        }
    }
};

class VectorPropertyExtractor {
public:
    VectorPropertyExtractor(CommitWriteBuffer::UntypedProperties& buf,
                            PropertyTypeID propID)
        : _buf(buf),
        _propID(propID)
    {
    }

    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const T& val : *typed) {
            _buf.emplace_back(_propID, std::make_optional(val));
        }
    }

    void operator()(const ColumnVector<types::String::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const types::String::Primitive val : *typed) {
            _buf.emplace_back(_propID, std::make_optional(std::string(val)));
        }
    }

    void operator()(const ColumnVector<types::Embedding::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const types::Embedding::Primitive val : *typed) {
            _buf.emplace_back(_propID, std::make_optional(types::Embedding::OwningPrimitive(val.begin(), val.end())));
        }
    }

    void operator()(const ColumnVector<ListView>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const ListView val : *typed) {
            throwIfHoldsAnUnstorableValue(val);
            _buf.emplace_back(_propID, types::List::OwningPrimitive(val));
        }
    }

    void operator()(const ColumnVector<MapView>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const MapView val : *typed) {
            throwIfHoldsAnUnstorableValue(val);
            _buf.emplace_back(_propID, types::Map::OwningPrimitive(val));
        }
    }

    template <typename T>
    void operator()(const ColumnOptVector<T>* typed) {
        const size_t rowCount = typed->size();
        _buf.clear();
        _buf.reserve(rowCount);
        for (size_t row = 0; row < rowCount; row++) {
            _buf.emplace_back(_propID, typed->get(row));
        }
    }

    /// Owning string as outlives query
    void operator()(const ColumnOptVector<types::String::Primitive>* typed) {
        const size_t rowCount = typed->size();
        _buf.clear();
        _buf.reserve(rowCount);
        for (size_t row = 0; row < rowCount; row++) {
            if (!typed->isValid(row)) {
                using Disengaged = std::optional<types::String::OwningPrimitive>;
                _buf.emplace_back(_propID, Disengaged {});
                continue;
            }

            _buf.emplace_back(_propID, std::make_optional(std::string(typed->getValue(row))));
        }
    }

    /// Owning vec as outlives query
    void operator()(const ColumnOptVector<types::Embedding::Primitive>* typed) {
        const size_t rowCount = typed->size();
        _buf.clear();
        _buf.reserve(rowCount);
        for (size_t row = 0; row < rowCount; row++) {
            if (!typed->isValid(row)) {
                using Disengaged = std::optional<types::Embedding::OwningPrimitive>;
                _buf.emplace_back(_propID, Disengaged {});
                continue;
            }
            const types::Embedding::Primitive value = typed->getValue(row);
            types::Embedding::OwningPrimitive emb(value.begin(), value.end());
            _buf.emplace_back(_propID, std::make_optional(std::move(emb)));
        }
    }

    /// Owning encoding as outlives query
    void operator()(const ColumnOptVector<ListView>* typed) {
        const size_t rowCount = typed->size();
        _buf.clear();
        _buf.reserve(rowCount);
        for (size_t row = 0; row < rowCount; row++) {
            if (!typed->isValid(row)) {
                using Disengaged = std::optional<types::List::OwningPrimitive>;
                _buf.emplace_back(_propID, Disengaged {});
                continue;
            }

            const ListView value = typed->getValue(row);
            throwIfHoldsAnUnstorableValue(value);
            _buf.emplace_back(_propID, types::List::OwningPrimitive(value));
        }
    }

    /// Owning encoding as outlives query
    void operator()(const ColumnOptVector<MapView>* typed) {
        const size_t rowCount = typed->size();
        _buf.clear();
        _buf.reserve(rowCount);
        for (size_t row = 0; row < rowCount; row++) {
            if (!typed->isValid(row)) {
                using Disengaged = std::optional<types::Map::OwningPrimitive>;
                _buf.emplace_back(_propID, Disengaged {});
                continue;
            }

            const MapView value = typed->getValue(row);
            throwIfHoldsAnUnstorableValue(value);
            _buf.emplace_back(_propID, types::Map::OwningPrimitive(value));
        }
    }

private:
    CommitWriteBuffer::UntypedProperties& _buf;
    PropertyTypeID _propID;
};

void disengagedValue(ValueType valueType, CommitWriteBuffer::SupportedTypeVariant& value) {
    const auto disengage = [&]<SupportedType T>() {
        if constexpr (TrivialSupportedType<T>) {
            value = std::optional<typename T::Primitive> {};
        } else {
            value = std::optional<typename T::OwningPrimitive> {};
        }
    };

    ValueTypeDispatcher(valueType).execute(disengage);
}

CellTag cellTagOf(ValueType valueType) {
    CellTag tag {CellTag::INVALID};

    const auto tagOf = [&]<SupportedType T>() {
        tag = TypeToCellTag<typename T::Primitive>::Tag;
    };

    ValueTypeDispatcher(valueType).execute(tagOf);

    return tag;
}

void stageCellAsItsType(const CellView cell,
                        ValueType valueType,
                        CommitWriteBuffer::SupportedTypeVariant& value) {
    switch (valueType) {
        case ValueType::Int64:
            value = std::optional {cell.getAs<types::Int64::Primitive>()};
        break;
        case ValueType::UInt64:
            value = std::optional {cell.getAs<types::UInt64::Primitive>()};
        break;
        case ValueType::Double:
            value = std::optional {cell.getAs<types::Double::Primitive>()};
        break;
        case ValueType::Bool:
            value = std::optional {cell.getAs<types::Bool::Primitive>()};
        break;
        case ValueType::DateTime:
            value = std::optional {cell.getAs<types::DateTime::Primitive>()};
        break;
        case ValueType::Duration:
            value = std::optional {cell.getAs<types::Duration::Primitive>()};
        break;
        case ValueType::Point:
            value = std::optional {cell.getAs<types::Point::Primitive>()};
        break;
        case ValueType::String:
            value = std::optional<types::String::OwningPrimitive> {std::in_place, cell.getAs<types::String::Primitive>()};
        break;
        case ValueType::Embedding: {
            const types::Embedding::Primitive embedding = cell.getAs<types::Embedding::Primitive>();
            value = std::optional<types::Embedding::OwningPrimitive> {std::in_place, embedding.begin(), embedding.end()};
        }
        break;
        case ValueType::List:
            throwIfHoldsAnUnstorableValue(cell.getAs<types::List::Primitive>());
            value = std::optional<types::List::OwningPrimitive> {std::in_place, cell.getAs<types::List::Primitive>()};
        break;
        case ValueType::Map:
            throwIfHoldsAnUnstorableValue(cell.getAs<types::Map::Primitive>());
            value = std::optional<types::Map::OwningPrimitive> {std::in_place, cell.getAs<types::Map::Primitive>()};
        break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("Cannot write a cell to a property of invalid type");
        break;
    }
}

// An integer widens to the other numeric types an integer literal can be written to
bool widensTo(CellTag tag, ValueType valueType) {
    return tag == CellTag::Int && (valueType == ValueType::Double || valueType == ValueType::UInt64);
}

types::UInt64::Primitive unsignedIntegerOf(types::Int64::Primitive integer) {
    if (integer < 0) {
        throw IRException(fmt::format("Cannot set a property of type '{}' to the negative integer {}",
                                      ValueTypeName::value(ValueType::UInt64),
                                      integer));
    }

    return static_cast<types::UInt64::Primitive>(integer);
}

// False for a cell of another type, which stages nothing
bool stageCellIfItFits(const CellView cell,
                       ValueType valueType,
                       CommitWriteBuffer::SupportedTypeVariant& value) {
    const CellTag tag = cell.getTag();
    const bool widens = widensTo(tag, valueType);

    if (tag == CellTag::Null) {
        disengagedValue(valueType, value);
    } else if (widens && valueType == ValueType::Double) {
        value = std::optional {static_cast<types::Double::Primitive>(cell.getAs<types::Int64::Primitive>())};
    } else if (widens) {
        value = std::optional {unsignedIntegerOf(cell.getAs<types::Int64::Primitive>())};
    } else if (tag == cellTagOf(valueType)) {
        stageCellAsItsType(cell, valueType, value);
    } else {
        return false;
    }

    return true;
}

void stageCell(const CellView cell,
               ValueType valueType,
               CommitWriteBuffer::SupportedTypeVariant& value) {
    if (!stageCellIfItFits(cell, valueType, value)) {
        throw IRException(fmt::format("Cannot set a property of type '{}' to a value of type '{}'",
                                      ValueTypeName::value(valueType),
                                      queryTypeName(cell.getTag())));
    }
}

WrittenMap writtenMap(const MapView& map) {
    return WrittenMap {map};
}

WrittenMap writtenMap(const std::optional<MapView>& map) {
    if (!map) {
        return WrittenMap {std::nullopt, CellTag::Null};
    }

    return WrittenMap {map};
}

WrittenMap writtenMap(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::MapView) {
        return WrittenMap {cell.getAs<MapView>()};
    }

    return WrittenMap {std::nullopt, tag};
}

template <typename ColumnType>
void readWrittenMaps(const Column* column, std::vector<WrittenMap>& maps) {
    const ColumnType* typedColumn = static_cast<const ColumnType*>(column);
    const size_t rowCount = typedColumn->size();

    maps.resize(rowCount);
    for (size_t row = 0; row < rowCount; row++) {
        maps[row] = writtenMap(operandRow(typedColumn, row));
    }
}

template <typename ColumnType>
void readWrittenConstMap(const Column* column, size_t rowCount, std::vector<WrittenMap>& maps) {
    const ColumnType* constant = static_cast<const ColumnType*>(column);

    maps.assign(rowCount, writtenMap(constantOperand(constant)));
}

// The cell of each row @param written holds, or of every row for a null mask. A constant
// column holds its one cell on each of the @param rowCount rows.
template <typename Visit>
void visitWrittenCells(const Column* column, size_t rowCount, const ColumnVector<CustomBool>* written, Visit visit) {
    const ColumnKind::Code kind = column->getKind();

    const auto visitRows = [&](const auto& cellAt) {
        for (size_t row = 0; row < rowCount; row++) {
            if (writeTouchesRow(written, row)) {
                visit(row, cellAt(row));
            }
        }
    };

    if (kind == ColumnConst<CellView>::staticKind()) {
        const CellView cell = static_cast<const ColumnConst<CellView>*>(column)->getRaw();
        visitRows([cell](size_t) { return cell; });
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        const auto& cells = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
        visitRows([&cells](size_t row) { return cells[row]; });
    } else {
        throw IRException("A property write of cells reads a column of cells");
    }
}

// The value type a cell tagged @param tag is staged as, the inverse of cellTagOf: Invalid
// for a tag no property holds
ValueType cellValueType(CellTag tag) {
    for (size_t index = 1; index < static_cast<size_t>(ValueType::_SIZE); index++) {
        const ValueType valueType = static_cast<ValueType>(index);

        if (cellTagOf(valueType) == tag) {
            return valueType;
        }
    }

    return ValueType::Invalid;
}

// The type a property no write has typed takes once it holds a value tagged @param tag
// beside values of type @param widest: the one stageCellIfItFits stages all of them as
ValueType widenCellType(ValueType widest, CellTag tag, std::string_view propertyName) {
    const bool stagesAsWidest = widest != ValueType::Invalid
                             && (tag == cellTagOf(widest) || widensTo(tag, widest));

    if (tag == CellTag::Null || stagesAsWidest) {
        return widest;
    }

    const ValueType own = cellValueType(tag);
    const bool holdsIntegersAlone = widest == ValueType::Int64;

    if (own == ValueType::Invalid) {
        throw IRException(fmt::format("Cannot evaluate property: unsupported type '{}'", queryTypeName(tag)));
    } else if (widest == ValueType::Invalid) {
        return own;
    } else if (holdsIntegersAlone && widensTo(CellTag::Int, own)) {
        return own;
    }

    throw IRException(fmt::format("One write gives the new property '{}' two types: '{}' and '{}'",
                                  propertyName,
                                  queryTypeName(widest),
                                  queryTypeName(tag)));
}

const std::optional<PropertyType>& resolveMapKeyType(const GraphView* view,
                                                     const MetadataBuilder* metadataBuilder,
                                                     std::string_view key,
                                                     MapKeyPropertyType& keyType) {
    const bool resolvesTheKey = !keyType._resolved || keyType._key != key;

    if (resolvesTheKey) {
        keyType._key = key;
        keyType._type = findWrittenPropertyType(view, metadataBuilder, key);
        keyType._resolved = true;
    }

    return keyType._type;
}

}

bool db::writeTouchesRow(const ColumnVector<CustomBool>* rows, size_t row) {
    return !rows || (*rows)[row];
}

bool db::holdsValue(const CommitWriteBuffer::SupportedTypeVariant& value) {
    return std::visit([](const auto& held) { return held.has_value(); }, value);
}

std::optional<PropertyType> db::findWrittenPropertyType(const GraphView* view,
                                                        const MetadataBuilder* metadataBuilder,
                                                        std::string_view name) {
    const GraphMetadata& metadata = view->metadata();
    const PropertyTypeMap& propertyTypes = metadata.propTypes();

    const std::optional<PropertyType> graphType = propertyTypes.get(name);
    if (graphType || !metadataBuilder) {
        return graphType;
    }

    return metadataBuilder->findPropertyType(name);
}

std::optional<PropertyType> db::findWrittenPropertyType(const GraphView* view,
                                                        const MetadataBuilder* metadataBuilder,
                                                        PropertyTypeID id) {
    const GraphMetadata& metadata = view->metadata();
    const PropertyTypeMap& propertyTypes = metadata.propTypes();

    const std::optional<PropertyType> graphType = propertyTypes.get(id);
    if (graphType || !metadataBuilder) {
        return graphType;
    }

    return metadataBuilder->findPropertyType(id);
}

void db::extractWrittenMaps(const Column* column,
                            size_t rowCount,
                            std::vector<WrittenMap>& maps) {
    const ColumnKind::Code kind = column->getKind();

    if (kind == ColumnVector<MapView>::staticKind()) {
        readWrittenMaps<ColumnVector<MapView>>(column, maps);
    } else if (kind == ColumnOptVector<MapView>::staticKind()) {
        readWrittenMaps<ColumnOptVector<MapView>>(column, maps);
    } else if (kind == ColumnConst<MapView>::staticKind()) {
        readWrittenConstMap<ColumnConst<MapView>>(column, rowCount, maps);
    } else if (kind == ColumnConst<std::optional<MapView>>::staticKind()) {
        readWrittenConstMap<ColumnConst<std::optional<MapView>>>(column, rowCount, maps);
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        readWrittenMaps<ColumnVector<CellView>>(column, maps);
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        readWrittenConstMap<ColumnConst<CellView>>(column, rowCount, maps);
    } else {
        throw IRException("A properties write reads a column of maps or of cells");
    }
}

void db::forgetUntypedMapKeys(std::vector<MapKeyPropertyType>& keyTypes) {
    for (MapKeyPropertyType& keyType : keyTypes) {
        if (!keyType._type) {
            keyType._resolved = false;
        }
    }
}

void db::widenCreatedMapKeyTypes(const WrittenMap& map,
                                 const GraphView* view,
                                 const MetadataBuilder* metadataBuilder,
                                 std::vector<MapKeyPropertyType>& keyTypes,
                                 std::vector<MapKeyCreatedType>& created) {
    if (!map._map) {
        return;
    }

    const std::span<const MapEntryView> entries = map._map->entries();
    if (keyTypes.size() < entries.size()) {
        keyTypes.resize(entries.size());
    }

    for (size_t index = 0; index < entries.size(); index++) {
        const MapEntryView& entry = entries[index];
        const std::string_view key = entry.getKey();
        const CellView value = entry.getValue();
        const CellTag tag = value.getTag();

        if (tag == CellTag::Null) {
            continue;
        }

        const std::optional<PropertyType>& propertyType = resolveMapKeyType(view, metadataBuilder, key, keyTypes[index]);
        if (propertyType) {
            continue;
        }

        auto createdKey = std::ranges::find(created, key, &MapKeyCreatedType::_key);
        if (createdKey == created.end()) {
            createdKey = created.insert(created.end(), MapKeyCreatedType {std::string(key), ValueType::Invalid});
        }

        createdKey->_type = widenCellType(createdKey->_type, tag, key);
    }
}

void db::stageMapProperties(const WrittenMap& map,
                            const GraphView* view,
                            const MetadataBuilder* metadataBuilder,
                            std::vector<MapKeyPropertyType>& keyTypes,
                            CommitWriteBuffer::UntypedProperties& staged) {
    const bool holdsAMap = map._tag == CellTag::MapView || map._tag == CellTag::Null;
    if (!holdsAMap) {
        throw IRException(fmt::format("Cannot set the properties of an entity to a value of type '{}', which is not a map",
                                      queryTypeName(map._tag)));
    }

    if (!map._map) {
        return;
    }

    const std::span<const MapEntryView> entries = map._map->entries();
    if (keyTypes.size() < entries.size()) {
        keyTypes.resize(entries.size());
    }

    for (size_t index = 0; index < entries.size(); index++) {
        const MapEntryView& entry = entries[index];
        const std::string_view key = entry.getKey();
        const CellView value = entry.getValue();

        const std::optional<PropertyType>& propertyType = resolveMapKeyType(view, metadataBuilder, key, keyTypes[index]);
        const bool removesNothing = !propertyType && value.getTag() == CellTag::Null;

        if (removesNothing) {
            continue;
        }

        bioassert(propertyType, "The property '{}' a map writes has no type", key);

        CommitWriteBuffer::UntypedProperty& property = staged.emplace_back(propertyType->_id);
        if (!stageCellIfItFits(value, propertyType->_valueType, property.value)) {
            throw IRException(fmt::format("Cannot set property '{}' of type '{}' to a value of type '{}'",
                                          key,
                                          ValueTypeName::value(propertyType->_valueType),
                                          queryTypeName(value.getTag())));
        }
    }
}

void db::stageRemovals(std::span<const PropertyTypeID> held,
                       std::span<const CommitWriteBuffer::UntypedProperty> written,
                       const GraphView* view,
                       const MetadataBuilder* metadataBuilder,
                       std::vector<ValueType>& heldTypes,
                       CommitWriteBuffer::UntypedProperties& removals) {
    for (const PropertyTypeID id : held) {
        const auto writes = std::ranges::find(written, id, &CommitWriteBuffer::UntypedProperty::propertyID);
        if (writes != written.end()) {
            continue;
        }

        const size_t index = id.getValue();
        if (index >= heldTypes.size()) {
            heldTypes.resize(index + 1, ValueType::Invalid);
        }

        if (heldTypes[index] == ValueType::Invalid) {
            const std::optional<PropertyType> propertyType = findWrittenPropertyType(view, metadataBuilder, id);
            bioassert(propertyType, "Property type {} is held by an entity and known to no metadata", index);

            heldTypes[index] = propertyType->_valueType;
        }

        CommitWriteBuffer::UntypedProperty& property = removals.emplace_back(id);
        disengagedValue(heldTypes[index], property.value);
    }
}

void db::fillNullProperties(size_t rowCount,
                            PropertyTypeID propID,
                            ValueType valueType,
                            CommitWriteBuffer::UntypedProperties& buf) {
    CommitWriteBuffer::UntypedProperty null {propID, {}};
    disengagedValue(valueType, null.value);

    buf.assign(rowCount, null);
}

void db::extractColumnProperties(const Column* column,
                                 size_t rowCount,
                                 PropertyTypeID propID,
                                 CommitWriteBuffer::UntypedProperties& buf) {
    using Types = WriteProcessorPropertyTypes;

    const ContainerKind::Code containerKind = ColumnKind::extractContainerKind(column->getKind());

    if (containerKind == ContainerKind::code<ColumnConst>()) {
        ConstPropertyExtractor extractor(buf, propID, rowCount);
        ColumnSingleDispatcher<Types::AllowedConst,
                               ConstPropertyExtractor,
                               Types::ExcludedConst>::dispatch(column, extractor);
    } else {
        VectorPropertyExtractor extractor(buf, propID);
        ColumnSingleDispatcher<Types::AllowedVector,
                               VectorPropertyExtractor,
                               Types::ExcludedVector>::dispatch(column, extractor);
    }
}

ValueType db::cellPropertyType(const Column* column,
                               size_t rowCount,
                               const ColumnVector<CustomBool>* written,
                               std::string_view propertyName) {
    ValueType widest {ValueType::Invalid};

    visitWrittenCells(column, rowCount, written, [&](size_t row, const CellView cell) {
        widest = widenCellType(widest, cell.getTag(), propertyName);
    });

    return widest;
}

void db::extractCellProperties(const Column* column,
                               size_t rowCount,
                               const ColumnVector<CustomBool>* written,
                               PropertyTypeID propID,
                               ValueType valueType,
                               CommitWriteBuffer::UntypedProperties& buf) {
    buf.assign(rowCount, CommitWriteBuffer::UntypedProperty {propID, {}});

    visitWrittenCells(column, rowCount, written, [&](size_t row, const CellView cell) {
        stageCell(cell, valueType, buf[row].value);
    });
}
