#include "NLWriteProperties.h"

#include <algorithm>
#include <optional>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "columns/AllowedKinds.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnKind.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListElementView.h"
#include "list/ListUtils.h"
#include "map/MapUtils.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "IRException.h"

#include "IRException.h"

using namespace db;

namespace {

template <SupportedType T>
struct StagedPropertyValue {
    using Type = std::optional<typename T::Primitive>;
};

template <SupportedType T>
    requires (!TrivialSupportedType<T>)
struct StagedPropertyValue<T> {
    using Type = std::optional<typename T::OwningPrimitive>;
};

template <typename T>
T cellValue(const ListElementView cell) {
    return cell.getAs<T>();
}

template <typename T>
T cellValue(const MapEntryView entry) {
    return entry.getValueAs<T>();
}

// The value one tagged cell - a list element or a map entry - stages for a property holding
// T. The analyzer lets an integer set a double or unsigned property, so an integer cell is
// converted as that integer is.
template <SupportedType T>
class TaggedCellStager {
public:
    template <typename Cell, typename View>
    CommitWriteBuffer::SupportedTypeVariant operator()(const View cell) const {
        using Primitive = typename T::Primitive;
        using Staged = typename StagedPropertyValue<T>::Type;

        constexpr bool readsAnInteger = std::same_as<Cell, types::Int64::Primitive>
                                     || std::same_as<Cell, types::UInt64::Primitive>;
        constexpr bool writesANumber = std::same_as<T, types::Int64>
                                    || std::same_as<T, types::UInt64>
                                    || std::same_as<T, types::Double>;

        if constexpr (std::same_as<Cell, PropertyNull>) {
            return Staged {};
        } else if constexpr (std::same_as<T, types::Embedding> && std::same_as<Cell, Primitive>) {
            const Primitive embedding = cellValue<Primitive>(cell);
            return Staged {std::in_place, embedding.begin(), embedding.end()};
        } else if constexpr (std::same_as<Cell, Primitive>) {
            return Staged {std::in_place, cellValue<Primitive>(cell)};
        } else if constexpr (readsAnInteger && writesANumber) {
            return Staged {static_cast<Primitive>(cellValue<Cell>(cell))};
        } else {
            throw IRException(fmt::format("Cannot write a value of another type to a property of type '{}'",
                                          ValueTypeName::value(T::_valueType)));
        }
    }
};

template <SupportedType T>
void stageTaggedCellAs(const ListElementView cell, CommitWriteBuffer::SupportedTypeVariant& staged) {
    const ListTagDispatcher dispatcher {cell.getTag()};

    staged = dispatcher.execute(TaggedCellStager<T> {}, cell);
}

CommitWriteBuffer::SupportedTypeVariant disengagedValue(ValueType valueType) {
    CommitWriteBuffer::SupportedTypeVariant disengaged;

    const auto select = [&disengaged]<SupportedType T>() {
        if constexpr (TrivialSupportedType<T>) {
            disengaged = std::optional<typename T::Primitive> {};
        } else {
            disengaged = std::optional<typename T::OwningPrimitive> {};
        }
    };

    ValueTypeDispatcher(valueType).execute(select);

    return disengaged;
}

bool writesInto(ValueType propertyType, ValueType valueType) {
    const bool widensAnInteger = valueType == ValueType::Int64
                              && (propertyType == ValueType::UInt64 || propertyType == ValueType::Double);

    return propertyType == valueType || widensAnInteger;
}

ValueType mapEntryValueType(const MapEntryView entry) {
    switch (entry.getValueTag()) {
        case MapBufferTypeTag::Int:
        case MapBufferTypeTag::UInt:
            return ValueType::Int64;
        break;
        case MapBufferTypeTag::Double:
            return ValueType::Double;
        break;
        case MapBufferTypeTag::Bool:
            return ValueType::Bool;
        break;
        case MapBufferTypeTag::String:
            return ValueType::String;
        break;
        case MapBufferTypeTag::Embedding:
            return ValueType::Embedding;
        break;
        case MapBufferTypeTag::ListView:
            return ValueType::List;
        break;
        case MapBufferTypeTag::MapView:
            return ValueType::Map;
        break;
        case MapBufferTypeTag::DateTime:
            return ValueType::DateTime;
        break;
        case MapBufferTypeTag::Null:
            return ValueType::Invalid;
        break;
        case MapBufferTypeTag::NodeID:
        case MapBufferTypeTag::EdgeID:
            throw IRException("A node or an edge cannot be the value of a property");
        break;
        case MapBufferTypeTag::INVALID:
        break;
    }

    throw IRException("Unknown tag in a map entry");
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
        fillRows(typed->getRaw());
    }

    void operator()(const ColumnConst<std::optional<types::String::Primitive>>* typed) {
        const std::optional<types::String::Primitive>& value = typed->getRaw();

        std::optional<types::String::OwningPrimitive> owned;
        if (value.has_value()) {
            owned.emplace(*value);
        }

        fillRows(owned);
    }

    void operator()(const ColumnConst<std::optional<types::Embedding::Primitive>>* typed) {
        const std::optional<types::Embedding::Primitive>& value = typed->getRaw();

        std::optional<types::Embedding::OwningPrimitive> owned;
        if (value.has_value()) {
            owned.emplace(value->begin(), value->end());
        }

        fillRows(owned);
    }

    void operator()(const ColumnConst<std::optional<ListView>>* typed) {
        const std::optional<ListView>& value = typed->getRaw();

        std::optional<types::List::OwningPrimitive> owned;
        if (value.has_value()) {
            owned.emplace(*value);
        }

        fillRows(owned);
    }

    void operator()(const ColumnConst<MapView>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
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
            _buf.emplace_back(_propID, types::List::OwningPrimitive(val));
        }
    }

    void operator()(const ColumnVector<MapView>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const MapView val : *typed) {
            _buf.emplace_back(_propID, types::Map::OwningPrimitive(val));
        }
    }

    template <typename T>
    void operator()(const ColumnVector<std::optional<T>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<T>& val : *typed) {
            _buf.emplace_back(_propID, val);
        }
    }

    /// Owning string as outlives query
    void operator()(const ColumnVector<std::optional<types::String::Primitive>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<types::String::Primitive>& val : *typed) {
            if (!val.has_value()) {
                using Disengaged = std::optional<types::String::OwningPrimitive>;
                _buf.emplace_back(_propID, Disengaged {});
                continue;
            }

            _buf.emplace_back(_propID, std::make_optional(std::string(*val)));
        }
    }

    /// Owning vec as outlives query
    void operator()(const ColumnVector<std::optional<types::Embedding::Primitive>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<types::Embedding::Primitive>& val : *typed) {
            if (!val.has_value()) {
                using Disengaged = std::optional<types::Embedding::OwningPrimitive>;
                _buf.emplace_back(_propID, Disengaged {});
                continue;
            }
            types::Embedding::OwningPrimitive emb(val->begin(), val->end());
            _buf.emplace_back(_propID, std::make_optional(std::move(emb)));
        }
    }

    /// Owning encoding as outlives query
    void operator()(const ColumnVector<std::optional<ListView>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<ListView>& val : *typed) {
            if (!val.has_value()) {
                using Disengaged = std::optional<types::List::OwningPrimitive>;
                _buf.emplace_back(_propID, Disengaged {});
                continue;
            }

            _buf.emplace_back(_propID, types::List::OwningPrimitive(*val));
        }
    }

    /// Owning encoding as outlives query
    void operator()(const ColumnVector<std::optional<MapView>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<MapView>& val : *typed) {
            if (!val.has_value()) {
                using Disengaged = std::optional<types::Map::OwningPrimitive>;
                _buf.emplace_back(_propID, Disengaged {});
                continue;
            }

            _buf.emplace_back(_propID, types::Map::OwningPrimitive(*val));
        }
    }

private:
    CommitWriteBuffer::UntypedProperties& _buf;
    PropertyTypeID _propID;
};

// The analyzer lets an integer be written to a double property, and a column can carry an
// integer unsigned, so a number is staged as the property's own type: the commit files a
// value under the type its variant holds.
template <typename Target>
void convertNumbers(CommitWriteBuffer::UntypedProperties& buf) {
    for (CommitWriteBuffer::UntypedProperty& property : buf) {
        const auto convert = [&property](const auto& held) {
            using Held = typename std::decay_t<decltype(held)>::value_type;

            if constexpr (std::is_arithmetic_v<Held> && !std::same_as<Held, Target>) {
                property.value = held ? std::optional<Target> {static_cast<Target>(*held)} : std::optional<Target> {};
            }
        };

        std::visit(convert, property.value);
    }
}

void extractMaskProperties(const ColumnMask* mask,
                           PropertyTypeID propID,
                           CommitWriteBuffer::UntypedProperties& buf) {
    buf.clear();
    buf.reserve(mask->size());
    for (const ColumnMask::Bool_t flag : mask->getRaw()) {
        const std::optional<types::Bool::Primitive> val {flag._value};
        buf.emplace_back(propID, val);
    }
}

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

ListBufferTypeTag listBufferTag(ValueType valueType) {
    ListBufferTypeTag tag {ListBufferTypeTag::INVALID};

    const auto tagOf = [&]<SupportedType T>() {
        tag = TypeToListBufferTag<typename T::Primitive>::Tag;
    };

    ValueTypeDispatcher(valueType).execute(tagOf);

    return tag;
}

void stageListElementAsItsType(const ListElementView element,
                               ValueType valueType,
                               CommitWriteBuffer::SupportedTypeVariant& value) {
    switch (valueType) {
        case ValueType::Int64:
            value = std::optional {element.getAs<types::Int64::Primitive>()};
        break;
        case ValueType::UInt64:
            value = std::optional {element.getAs<types::UInt64::Primitive>()};
        break;
        case ValueType::Double:
            value = std::optional {element.getAs<types::Double::Primitive>()};
        break;
        case ValueType::Bool:
            value = std::optional {element.getAs<types::Bool::Primitive>()};
        break;
        case ValueType::DateTime:
            value = std::optional {element.getAs<types::DateTime::Primitive>()};
        break;
        case ValueType::String:
            value = std::optional<types::String::OwningPrimitive> {std::in_place, element.getAs<types::String::Primitive>()};
        break;
        case ValueType::Embedding: {
            const types::Embedding::Primitive embedding = element.getAs<types::Embedding::Primitive>();
            value = std::optional<types::Embedding::OwningPrimitive> {std::in_place, embedding.begin(), embedding.end()};
        }
        break;
        case ValueType::List:
            value = std::optional<types::List::OwningPrimitive> {std::in_place, element.getAs<types::List::Primitive>()};
        break;
        case ValueType::Map:
            value = std::optional<types::Map::OwningPrimitive> {std::in_place, element.getAs<types::Map::Primitive>()};
        break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("Cannot write a list element to a property of invalid type");
        break;
    }
}

// An integer widens to the other numeric types an integer literal can be written to
void stageListElement(const ListElementView element,
                      ValueType valueType,
                      CommitWriteBuffer::SupportedTypeVariant& value) {
    const ListBufferTypeTag tag = element.getTag();

    const bool holdsAnInteger = tag == ListBufferTypeTag::Int;
    const bool widensToDouble = holdsAnInteger && valueType == ValueType::Double;
    const bool widensToUnsigned = holdsAnInteger && valueType == ValueType::UInt64;

    if (tag == ListBufferTypeTag::Null) {
        disengagedValue(valueType, value);
    } else if (widensToDouble) {
        value = std::optional {static_cast<types::Double::Primitive>(element.getAs<types::Int64::Primitive>())};
    } else if (widensToUnsigned) {
        value = std::optional {static_cast<types::UInt64::Primitive>(element.getAs<types::Int64::Primitive>())};
    } else if (tag == listBufferTag(valueType)) {
        stageListElementAsItsType(element, valueType, value);
    } else {
        throw IRException(fmt::format("Cannot set a property of type '{}' to a list element of another type",
                                      ValueTypeName::value(valueType)));
    }
}

void stageListElementRows(const ListElementView element,
                          size_t rowCount,
                          PropertyTypeID propID,
                          ValueType valueType,
                          CommitWriteBuffer::UntypedProperties& buf) {
    CommitWriteBuffer::UntypedProperty property {propID, {}};
    stageListElement(element, valueType, property.value);

    buf.assign(rowCount, property);
}

}

size_t db::committedNodeCount(const GraphView* view) {
    if (!view || !view->isValid()) {
        return 0;
    }

    const GraphReader reader = view->read();
    return reader.getTotalNodesAllocated();
}

size_t db::committedEdgeCount(const GraphView* view) {
    if (!view || !view->isValid()) {
        return 0;
    }

    const GraphReader reader = view->read();
    return reader.getTotalEdgesAllocated();
}

void db::fillNullProperties(size_t rowCount,
                            PropertyTypeID propID,
                            ValueType valueType,
                            CommitWriteBuffer::UntypedProperties& buf) {
    buf.assign(rowCount, CommitWriteBuffer::UntypedProperty {propID, disengagedValue(valueType)});
}

void db::extractColumnProperties(const Column* column,
                                 size_t rowCount,
                                 PropertyType property,
                                 CommitWriteBuffer::UntypedProperties& buf) {
    using Types = WriteProcessorPropertyTypes;

    const PropertyTypeID propID = property._id;

    const ContainerKind::Code containerKind = ColumnKind::extractContainerKind(column->getKind());

    if (containerKind == ContainerKind::code<ColumnConst>()) {
        ConstPropertyExtractor extractor(buf, propID, rowCount);
        ColumnSingleDispatcher<Types::AllowedConst,
                               ConstPropertyExtractor,
                               Types::ExcludedConst>::dispatch(column, extractor);
    } else if (containerKind == ContainerKind::code<ColumnMask>()) {
        extractMaskProperties(static_cast<const ColumnMask*>(column), propID, buf);
    } else {
        VectorPropertyExtractor extractor(buf, propID);
        ColumnSingleDispatcher<Types::AllowedVector,
                               VectorPropertyExtractor,
                               Types::ExcludedVector>::dispatch(column, extractor);
    }

    switch (property._valueType) {
        case ValueType::Int64:
            convertNumbers<types::Int64::Primitive>(buf);
        break;
        case ValueType::UInt64:
            convertNumbers<types::UInt64::Primitive>(buf);
        break;
        case ValueType::Double:
            convertNumbers<types::Double::Primitive>(buf);
        break;
        default:
        break;
    }
}

bool db::readsTaggedCells(const Column* column) {
    const ColumnKind::Code kind = column->getKind();

    return kind == ColumnVector<ListElementView>::staticKind()
        || kind == ColumnConst<ListElementView>::staticKind()
        || kind == ColumnOptVector<ListElementView>::staticKind()
        || kind == ColumnConst<std::optional<ListElementView>>::staticKind();
}

ListElementView db::taggedCellAt(const Column* column, size_t row) {
    const ColumnKind::Code kind = column->getKind();
    const ListElementView nullCell = ListElementView::nullElement();

    if (kind == ColumnConst<ListElementView>::staticKind()) {
        return static_cast<const ColumnConst<ListElementView>*>(column)->getRaw();
    } else if (kind == ColumnConst<std::optional<ListElementView>>::staticKind()) {
        return static_cast<const ColumnConst<std::optional<ListElementView>>*>(column)->getRaw().value_or(nullCell);
    } else if (kind == ColumnVector<ListElementView>::staticKind()) {
        return static_cast<const ColumnVector<ListElementView>*>(column)->getRaw()[row];
    } else {
        return static_cast<const ColumnOptVector<ListElementView>*>(column)->getRaw()[row].value_or(nullCell);
    }
}

ValueType db::taggedCellValueType(const ListElementView cell) {
    switch (cell.getTag()) {
        case ListBufferTypeTag::Int:
        case ListBufferTypeTag::UInt:
            return ValueType::Int64;
        break;
        case ListBufferTypeTag::Double:
            return ValueType::Double;
        break;
        case ListBufferTypeTag::Bool:
            return ValueType::Bool;
        break;
        case ListBufferTypeTag::String:
            return ValueType::String;
        break;
        case ListBufferTypeTag::Embedding:
            return ValueType::Embedding;
        break;
        case ListBufferTypeTag::ListView:
            return ValueType::List;
        break;
        case ListBufferTypeTag::DateTime:
            return ValueType::DateTime;
        break;
        case ListBufferTypeTag::MapView:
            return ValueType::Map;
        break;
        case ListBufferTypeTag::Null:
            return ValueType::Invalid;
        break;
        case ListBufferTypeTag::NodeID:
        case ListBufferTypeTag::EdgeID:
            throw IRException("A node or an edge cannot be the value of a property");
        break;
        case ListBufferTypeTag::INVALID:
        break;
    }

    throw IRException("Unknown tag in a tagged cell");
}

void db::stageTaggedCell(const ListElementView cell,
                         ValueType valueType,
                         CommitWriteBuffer::SupportedTypeVariant& staged) {
    const auto stage = [&]<SupportedType T>() {
        stageTaggedCellAs<T>(cell, staged);
    };

    ValueTypeDispatcher(valueType).execute(stage);
}

void db::stageTaggedCells(const Column* column,
                          size_t rowCount,
                          PropertyType property,
                          llvm::function_ref<bool(size_t)> stagesRow,
                          CommitWriteBuffer::UntypedProperties& buf) {
    buf.assign(rowCount, CommitWriteBuffer::UntypedProperty {property._id, {}});

    for (size_t row = 0; row < rowCount; row++) {
        if (stagesRow(row)) {
            stageTaggedCell(taggedCellAt(column, row), property._valueType, buf[row].value);
        }
    }
}

PropertyType db::resolveTaggedCellProperty(MetadataBuilder* metadataBuilder,
                                           std::string_view name,
                                           const Column* column,
                                           size_t rowCount,
                                           llvm::function_ref<bool(size_t)> stagesRow) {
    const std::optional<PropertyType> registered = metadataBuilder->findPropertyType(name);
    if (registered) {
        return *registered;
    }

    for (size_t row = 0; row < rowCount; row++) {
        const ValueType valueType = stagesRow(row) ? taggedCellValueType(taggedCellAt(column, row)) : ValueType::Invalid;
        if (valueType != ValueType::Invalid) {
            return metadataBuilder->getOrCreatePropertyType(name, valueType);
        }
    }

    return PropertyType {};
}

std::optional<MapView> db::mapCellAt(const Column* column, size_t row) {
    const ColumnKind::Code kind = column->getKind();

    if (readsTaggedCells(column)) {
        const ListElementView cell = taggedCellAt(column, row);
        const ListBufferTypeTag tag = cell.getTag();

        if (tag == ListBufferTypeTag::Null) {
            return std::nullopt;
        } else if (tag != ListBufferTypeTag::MapView) {
            throw IRException("SET n = m and SET n += m read a map of properties, and m holds no map");
        }

        return cell.getAs<MapView>();
    } else if (kind == ColumnConst<MapView>::staticKind()) {
        return static_cast<const ColumnConst<MapView>*>(column)->getRaw();
    } else if (kind == ColumnConst<std::optional<MapView>>::staticKind()) {
        return static_cast<const ColumnConst<std::optional<MapView>>*>(column)->getRaw();
    } else if (kind == ColumnVector<MapView>::staticKind()) {
        return static_cast<const ColumnVector<MapView>*>(column)->getRaw()[row];
    } else if (kind == ColumnOptVector<MapView>::staticKind()) {
        return static_cast<const ColumnOptVector<MapView>*>(column)->getRaw()[row];
    } else {
        throw IRException("SET reads its map from a column holding no maps");
    }
}

void db::stageMapEntries(const MapView map,
                         MetadataBuilder* metadataBuilder,
                         std::vector<PropertyType>& created,
                         CommitWriteBuffer::UntypedProperties& staged) {
    for (const MapEntryView entry : map) {
        const std::string_view key = entry.getKey();
        const ValueType valueType = mapEntryValueType(entry);
        const std::optional<PropertyType> registered = metadataBuilder->findPropertyType(key);

        if (valueType == ValueType::Invalid) {
            if (registered) {
                staged.push_back({registered->_id, disengagedValue(registered->_valueType)});
            }

            continue;
        }

        if (registered && !writesInto(registered->_valueType, valueType)) {
            throw IRException(fmt::format("Cannot write a value of type '{}' to the property '{}' of type '{}'",
                                          ValueTypeName::value(valueType),
                                          key,
                                          ValueTypeName::value(registered->_valueType)));
        }

        const PropertyType property = registered ? *registered : metadataBuilder->getOrCreatePropertyType(key, valueType);
        if (!registered) {
            created.push_back(property);
        }

        CommitWriteBuffer::SupportedTypeVariant value;
        const auto stage = [&value, entry]<SupportedType T>() {
            const MapTagDispatcher dispatcher {entry.getValueTag()};
            value = dispatcher.execute(TaggedCellStager<T> {}, entry);
        };

        ValueTypeDispatcher(property._valueType).execute(stage);

        staged.push_back({property._id, std::move(value)});
    }
}

void db::stageMapRemovals(std::span<const PropertyType> known, CommitWriteBuffer::UntypedProperties& staged) {
    const size_t entryCount = staged.size();

    for (const PropertyType property : known) {
        const auto setsTheProperty = [property](const CommitWriteBuffer::UntypedProperty& entry) {
            return entry.propertyID == property._id;
        };

        const bool setByAnEntry = std::any_of(staged.begin(), staged.begin() + entryCount, setsTheProperty);
        if (!setByAnEntry) {
            staged.push_back({property._id, disengagedValue(property._valueType)});
        }
    }
}
