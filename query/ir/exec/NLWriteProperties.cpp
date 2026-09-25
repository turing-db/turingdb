#include "NLWriteProperties.h"

#include <optional>

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

// The value one tagged cell stages for a property holding T. The analyzer lets an integer
// set a double or unsigned property, so an integer cell is converted as that integer is.
template <SupportedType T>
class TaggedCellStager {
public:
    template <typename Cell>
    CommitWriteBuffer::SupportedTypeVariant operator()(const ListElementView cell) const {
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
            const Primitive embedding = cell.getAs<Primitive>();
            return Staged {std::in_place, embedding.begin(), embedding.end()};
        } else if constexpr (std::same_as<Cell, Primitive>) {
            return Staged {std::in_place, cell.getAs<Primitive>()};
        } else if constexpr (readsAnInteger && writesANumber) {
            return Staged {static_cast<Primitive>(cell.getAs<Cell>())};
        } else {
            throw IRException(fmt::format("Cannot write a value of another type to a property of type '{}'",
                                          ValueTypeName::value(T::_valueType)));
        }
    }
};

template <SupportedType T>
CommitWriteBuffer::SupportedTypeVariant stageTaggedCell(const ListElementView cell) {
    const ListTagDispatcher dispatcher {cell.getTag()};

    return dispatcher.execute(TaggedCellStager<T> {}, cell);
}

// The type of property a tagged cell's value makes, Invalid for a null. Cypher has one
// integer type, and it is signed.
ValueType taggedCellValueType(const ListElementView cell) {
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

ValueType firstTaggedCellValueType(const Column* column) {
    const ColumnKind::Code kind = column->getKind();
    const ListElementView nullCell = ListElementView::nullElement();

    if (kind == ColumnConst<ListElementView>::staticKind()) {
        return taggedCellValueType(static_cast<const ColumnConst<ListElementView>*>(column)->getRaw());
    } else if (kind == ColumnConst<std::optional<ListElementView>>::staticKind()) {
        const std::optional<ListElementView>& cell =
            static_cast<const ColumnConst<std::optional<ListElementView>>*>(column)->getRaw();
        return taggedCellValueType(cell.value_or(nullCell));
    } else if (kind == ColumnVector<ListElementView>::staticKind()) {
        for (const ListElementView cell : static_cast<const ColumnVector<ListElementView>*>(column)->getRaw()) {
            const ValueType valueType = taggedCellValueType(cell);
            if (valueType != ValueType::Invalid) {
                return valueType;
            }
        }
    } else {
        const std::vector<std::optional<ListElementView>>& cells =
            static_cast<const ColumnOptVector<ListElementView>*>(column)->getRaw();

        for (const std::optional<ListElementView>& cell : cells) {
            const ValueType valueType = taggedCellValueType(cell.value_or(nullCell));
            if (valueType != ValueType::Invalid) {
                return valueType;
            }
        }
    }

    return ValueType::Invalid;
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
    } else if (containerKind == ContainerKind::code<ColumnMask>()) {
        extractMaskProperties(static_cast<const ColumnMask*>(column), propID, buf);
    } else {
        VectorPropertyExtractor extractor(buf, propID);
        ColumnSingleDispatcher<Types::AllowedVector,
                               VectorPropertyExtractor,
                               Types::ExcludedVector>::dispatch(column, extractor);
    }
}

void db::extractTaggedCellProperties(const Column* column,
                                     size_t rowCount,
                                     PropertyTypeID propID,
                                     ValueType valueType,
                                     CommitWriteBuffer::UntypedProperties& buf) {
    const ColumnKind::Code kind = column->getKind();
    const ListElementView nullCell = ListElementView::nullElement();

    const auto extract = [&]<SupportedType T>() {
        buf.clear();

        if (kind == ColumnConst<ListElementView>::staticKind()) {
            const ListElementView cell = static_cast<const ColumnConst<ListElementView>*>(column)->getRaw();
            buf.assign(rowCount, CommitWriteBuffer::UntypedProperty {propID, stageTaggedCell<T>(cell)});
        } else if (kind == ColumnConst<std::optional<ListElementView>>::staticKind()) {
            const std::optional<ListElementView>& cell =
                static_cast<const ColumnConst<std::optional<ListElementView>>*>(column)->getRaw();
            buf.assign(rowCount, CommitWriteBuffer::UntypedProperty {propID, stageTaggedCell<T>(cell.value_or(nullCell))});
        } else if (kind == ColumnVector<ListElementView>::staticKind()) {
            const std::vector<ListElementView>& cells = static_cast<const ColumnVector<ListElementView>*>(column)->getRaw();

            buf.reserve(cells.size());
            for (const ListElementView cell : cells) {
                buf.emplace_back(propID, stageTaggedCell<T>(cell));
            }
        } else {
            const std::vector<std::optional<ListElementView>>& cells =
                static_cast<const ColumnOptVector<ListElementView>*>(column)->getRaw();

            buf.reserve(cells.size());
            for (const std::optional<ListElementView>& cell : cells) {
                buf.emplace_back(propID, stageTaggedCell<T>(cell.value_or(nullCell)));
            }
        }
    };

    ValueTypeDispatcher(valueType).execute(extract);
}

PropertyType db::createTaggedCellProperty(MetadataBuilder* metadataBuilder,
                                          std::string_view name,
                                          const Column* column) {
    const std::optional<PropertyType> registered = metadataBuilder->findPropertyType(name);
    if (registered) {
        return *registered;
    }

    const ValueType valueType = firstTaggedCellValueType(column);

    return valueType == ValueType::Invalid ? PropertyType {} : metadataBuilder->getOrCreatePropertyType(name, valueType);
}
