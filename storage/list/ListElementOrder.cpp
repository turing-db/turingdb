#include "ListElementOrder.h"

#include <stdint.h>

#include <algorithm>
#include <cmath>
#include <type_traits>

#include "ID.h"
#include "ListBufferTypeTag.h"
#include "NumberOrder.h"

#include "map/MapBufferTypeTag.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"

#include "metadata/PropertyType.h"

#include "FatalException.h"

using namespace db;

namespace {

// The class a tagged element sorts in, ascending.
enum class ListElementOrderClass {
    Map = 0,
    Node,
    Edge,
    List,
    DateTime,
    Duration,
    String,
    Bool,
    Number,
    Null,
};

ListElementOrderClass orderClassOf(ListBufferTypeTag tag) {
    switch (tag) {
        case ListBufferTypeTag::NodeID:
            return ListElementOrderClass::Node;
        break;

        case ListBufferTypeTag::EdgeID:
            return ListElementOrderClass::Edge;
        break;

        case ListBufferTypeTag::ListView:
            return ListElementOrderClass::List;
        break;

        case ListBufferTypeTag::String:
            return ListElementOrderClass::String;
        break;

        case ListBufferTypeTag::Bool:
            return ListElementOrderClass::Bool;
        break;

        case ListBufferTypeTag::Int:
        case ListBufferTypeTag::UInt:
        case ListBufferTypeTag::Double:
            return ListElementOrderClass::Number;
        break;

        case ListBufferTypeTag::Null:
            return ListElementOrderClass::Null;
        break;

        case ListBufferTypeTag::DateTime:
            return ListElementOrderClass::DateTime;
        break;

        case ListBufferTypeTag::Duration:
            return ListElementOrderClass::Duration;
        break;

        case ListBufferTypeTag::Embedding:
            throw FatalException("Cannot order an embedding list element.");
        break;

        case ListBufferTypeTag::MapView:
            return ListElementOrderClass::Map;
        break;

        case ListBufferTypeTag::INVALID:
            throw FatalException("Cannot order an untagged list element.");
        break;
    }

    throw FatalException("Unknown ListBufferTypeTag.");
}

ListBufferTypeTag listTagOf(MapBufferTypeTag tag) {
    switch (tag) {
        case MapBufferTypeTag::Int:
            return ListBufferTypeTag::Int;
        break;
        case MapBufferTypeTag::UInt:
            return ListBufferTypeTag::UInt;
        break;
        case MapBufferTypeTag::Double:
            return ListBufferTypeTag::Double;
        break;
        case MapBufferTypeTag::Bool:
            return ListBufferTypeTag::Bool;
        break;
        case MapBufferTypeTag::String:
            return ListBufferTypeTag::String;
        break;
        case MapBufferTypeTag::Embedding:
            return ListBufferTypeTag::Embedding;
        break;
        case MapBufferTypeTag::ListView:
            return ListBufferTypeTag::ListView;
        break;
        case MapBufferTypeTag::MapView:
            return ListBufferTypeTag::MapView;
        break;
        case MapBufferTypeTag::Null:
            return ListBufferTypeTag::Null;
        break;
        case MapBufferTypeTag::NodeID:
            return ListBufferTypeTag::NodeID;
        break;
        case MapBufferTypeTag::EdgeID:
            return ListBufferTypeTag::EdgeID;
        break;
        case MapBufferTypeTag::DateTime:
            return ListBufferTypeTag::DateTime;
        break;
        case MapBufferTypeTag::Duration:
            return ListBufferTypeTag::Duration;
        break;
        case MapBufferTypeTag::INVALID:
            return ListBufferTypeTag::INVALID;
        break;
    }

    return ListBufferTypeTag::INVALID;
}

ListBufferTypeTag tagOf(const ListElementView element) {
    return element.getTag();
}

ListBufferTypeTag tagOf(const MapEntryView entry) {
    return listTagOf(entry.getValueTag());
}

template <typename T>
T valueOf(const ListElementView element) {
    return element.getAs<T>();
}

template <typename T>
T valueOf(const MapEntryView entry) {
    return entry.getValueAs<T>();
}

template <typename Value>
bool isNaN(const Value value) {
    if constexpr (std::is_floating_point_v<Value>) {
        return std::isnan(value);
    } else {
        return false;
    }
}

// Orders two numbers by their exact values, whatever their types. A NaN sorts after every
// number, as Cypher orders it.
template <typename Lhs, typename Rhs>
std::strong_ordering compareNumericValues(const Lhs lhs, const Rhs rhs) {
    const bool lhsIsNaN = isNaN(lhs);
    const bool rhsIsNaN = isNaN(rhs);

    if (lhsIsNaN || rhsIsNaN) {
        if (lhsIsNaN && rhsIsNaN) {
            return std::strong_ordering::equal;
        }

        return lhsIsNaN ? std::strong_ordering::greater : std::strong_ordering::less;
    }

    const std::partial_ordering order = numberOrder(lhs, rhs);
    if (order == std::partial_ordering::less) {
        return std::strong_ordering::less;
    } else if (order == std::partial_ordering::greater) {
        return std::strong_ordering::greater;
    }

    return std::strong_ordering::equal;
}

template <typename View, typename Value>
std::strong_ordering compareNumberWithValue(const View view, const Value value) {
    switch (tagOf(view)) {
        case ListBufferTypeTag::Int:
            return compareNumericValues(valueOf<types::Int64::Primitive>(view), value);
        break;

        case ListBufferTypeTag::UInt:
            return compareNumericValues(valueOf<types::UInt64::Primitive>(view), value);
        break;

        default:
            return compareNumericValues(valueOf<types::Double::Primitive>(view), value);
        break;
    }
}

template <typename Lhs, typename Rhs>
std::strong_ordering compareNumbers(const Lhs lhs, const Rhs rhs) {
    switch (tagOf(rhs)) {
        case ListBufferTypeTag::Int:
            return compareNumberWithValue(lhs, valueOf<types::Int64::Primitive>(rhs));
        break;

        case ListBufferTypeTag::UInt:
            return compareNumberWithValue(lhs, valueOf<types::UInt64::Primitive>(rhs));
        break;

        default:
            return compareNumberWithValue(lhs, valueOf<types::Double::Primitive>(rhs));
        break;
    }
}

// Orders two tagged values - two list elements, or the values of two map entries - on the
// cross-type order the header documents
template <typename Lhs, typename Rhs>
std::strong_ordering compareValues(const Lhs lhs, const Rhs rhs) {
    const ListElementOrderClass lhsClass = orderClassOf(tagOf(lhs));
    const ListElementOrderClass rhsClass = orderClassOf(tagOf(rhs));

    if (lhsClass != rhsClass) {
        return lhsClass <=> rhsClass;
    }

    switch (lhsClass) {
        case ListElementOrderClass::Map:
            return valueOf<MapView>(lhs) <=> valueOf<MapView>(rhs);
        break;

        case ListElementOrderClass::Node:
            return valueOf<NodeID>(lhs) <=> valueOf<NodeID>(rhs);
        break;

        case ListElementOrderClass::Edge:
            return valueOf<EdgeID>(lhs) <=> valueOf<EdgeID>(rhs);
        break;

        case ListElementOrderClass::List:
            return valueOf<ListView>(lhs) <=> valueOf<ListView>(rhs);
        break;

        case ListElementOrderClass::String:
            return valueOf<types::String::Primitive>(lhs) <=> valueOf<types::String::Primitive>(rhs);
        break;

        case ListElementOrderClass::Bool:
            return static_cast<bool>(valueOf<types::Bool::Primitive>(lhs))
                <=> static_cast<bool>(valueOf<types::Bool::Primitive>(rhs));
        break;

        case ListElementOrderClass::Number:
            return compareNumbers(lhs, rhs);
        break;

        case ListElementOrderClass::DateTime:
            return valueOf<types::DateTime::Primitive>(lhs) <=> valueOf<types::DateTime::Primitive>(rhs);
        break;

        case ListElementOrderClass::Duration:
            return valueOf<types::Duration::Primitive>(lhs) <=> valueOf<types::Duration::Primitive>(rhs);
        break;

        case ListElementOrderClass::Null:
            return std::strong_ordering::equal;
        break;
    }

    throw FatalException("Unknown list element order class.");
}

template <typename View, typename Value>
bool elementEqualsNumber(const View view, const Value value) {
    switch (tagOf(view)) {
        case ListBufferTypeTag::Int:
            return numbersEqual(valueOf<types::Int64::Primitive>(view), value);
        break;

        case ListBufferTypeTag::UInt:
            return numbersEqual(valueOf<types::UInt64::Primitive>(view), value);
        break;

        case ListBufferTypeTag::Double:
            return numbersEqual(valueOf<types::Double::Primitive>(view), value);
        break;

        default:
            return false;
        break;
    }
}

// Orders an element against a value of a known type: the element's class against the
// value's when the two differ, and the two values themselves when they agree.
template <typename Value>
std::strong_ordering compareElementWithNumber(const ListElementView element, const Value value) {
    const ListElementOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != ListElementOrderClass::Number) {
        return elementClass <=> ListElementOrderClass::Number;
    }

    return compareNumberWithValue(element, value);
}

// Embeddings have no order, so equality cannot be read off compareValues wherever one may
// sit - directly, or anywhere inside a nested list or map
template <typename Lhs, typename Rhs>
bool valuesEqual(const Lhs lhs, const Rhs rhs) {
    const ListBufferTypeTag lhsTag = tagOf(lhs);
    const ListBufferTypeTag rhsTag = tagOf(rhs);

    const bool lhsIsEmbedding = lhsTag == ListBufferTypeTag::Embedding;
    const bool rhsIsEmbedding = rhsTag == ListBufferTypeTag::Embedding;
    const bool bothMaps = lhsTag == ListBufferTypeTag::MapView && rhsTag == ListBufferTypeTag::MapView;
    const bool bothLists = lhsTag == ListBufferTypeTag::ListView && rhsTag == ListBufferTypeTag::ListView;

    if (lhsIsEmbedding || rhsIsEmbedding) {
        return lhsIsEmbedding && rhsIsEmbedding
            && EmbeddingEqual {}(valueOf<types::Embedding::Primitive>(lhs),
                                 valueOf<types::Embedding::Primitive>(rhs));
    } else if (bothMaps) {
        return valueOf<MapView>(lhs) == valueOf<MapView>(rhs);
    } else if (bothLists) {
        return valueOf<ListView>(lhs) == valueOf<ListView>(rhs);
    }

    return compareValues(lhs, rhs) == std::strong_ordering::equal;
}

template <typename Value>
std::optional<std::partial_ordering> elementNumberComparisonOrder(const ListElementView element, const Value value) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return numberOrder(element.getAs<types::Int64::Primitive>(), value);
        break;

        case ListBufferTypeTag::UInt:
            return numberOrder(element.getAs<types::UInt64::Primitive>(), value);
        break;

        case ListBufferTypeTag::Double:
            return numberOrder(element.getAs<types::Double::Primitive>(), value);
        break;

        default:
            return std::nullopt;
        break;
    }
}

}

std::strong_ordering db::operator<=>(const ListElementView lhs, const ListElementView rhs) {
    return compareValues(lhs, rhs);
}

bool db::operator==(const ListElementView lhs, const ListElementView rhs) {
    return valuesEqual(lhs, rhs);
}

std::strong_ordering db::operator<=>(const ListView lhs, const ListView rhs) {
    const std::span<const ListElementView> lhsElements = lhs.elements();
    const std::span<const ListElementView> rhsElements = rhs.elements();
    const size_t common = std::min(lhsElements.size(), rhsElements.size());

    for (size_t index = 0; index < common; index++) {
        const std::strong_ordering order = lhsElements[index] <=> rhsElements[index];
        if (order != std::strong_ordering::equal) {
            return order;
        }
    }

    return lhsElements.size() <=> rhsElements.size();
}

bool db::operator==(const ListView lhs, const ListView rhs) {
    const std::span<const ListElementView> lhsElements = lhs.elements();
    const std::span<const ListElementView> rhsElements = rhs.elements();
    if (lhsElements.size() != rhsElements.size()) {
        return false;
    }

    for (size_t index = 0; index < lhsElements.size(); index++) {
        if (!valuesEqual(lhsElements[index], rhsElements[index])) {
            return false;
        }
    }

    return true;
}

std::strong_ordering db::operator<=>(const MapView lhs, const MapView rhs) {
    const std::span<const MapEntryView> lhsEntries = lhs.entries();
    const std::span<const MapEntryView> rhsEntries = rhs.entries();
    const size_t common = std::min(lhsEntries.size(), rhsEntries.size());

    for (size_t index = 0; index < common; index++) {
        const MapEntryView lhsEntry = lhsEntries[index];
        const MapEntryView rhsEntry = rhsEntries[index];

        const std::strong_ordering keyOrder = lhsEntry.getKey() <=> rhsEntry.getKey();
        if (keyOrder != std::strong_ordering::equal) {
            return keyOrder;
        }

        const std::strong_ordering valueOrder = compareValues(lhsEntry, rhsEntry);
        if (valueOrder != std::strong_ordering::equal) {
            return valueOrder;
        }
    }

    return lhsEntries.size() <=> rhsEntries.size();
}

bool db::operator==(const MapView lhs, const MapView rhs) {
    const std::span<const MapEntryView> lhsEntries = lhs.entries();
    const std::span<const MapEntryView> rhsEntries = rhs.entries();
    if (lhsEntries.size() != rhsEntries.size()) {
        return false;
    }

    for (size_t index = 0; index < lhsEntries.size(); index++) {
        const MapEntryView lhsEntry = lhsEntries[index];
        const MapEntryView rhsEntry = rhsEntries[index];

        const bool sameEntry = lhsEntry.getKey() == rhsEntry.getKey() && valuesEqual(lhsEntry, rhsEntry);
        if (!sameEntry) {
            return false;
        }
    }

    return true;
}

bool db::operator==(const ListElementView element, const types::Int64::Primitive value) {
    return elementEqualsNumber(element, value);
}

bool db::operator==(const ListElementView element, const types::UInt64::Primitive value) {
    return elementEqualsNumber(element, value);
}

bool db::operator==(const ListElementView element, const types::Double::Primitive value) {
    return elementEqualsNumber(element, value);
}

bool db::operator==(const ListElementView element, const types::String::Primitive value) {
    return element.getTag() == ListBufferTypeTag::String
        && element.getAs<types::String::Primitive>() == value;
}

bool db::operator==(const ListElementView element, const types::Bool::Primitive value) {
    return element.getTag() == ListBufferTypeTag::Bool
        && static_cast<bool>(element.getAs<types::Bool::Primitive>()) == static_cast<bool>(value);
}

bool db::operator==(const ListElementView element, const types::DateTime::Primitive value) {
    return element.getTag() == ListBufferTypeTag::DateTime
        && element.getAs<types::DateTime::Primitive>() == value;
}

bool db::operator==(const ListElementView element, const types::Duration::Primitive value) {
    return element.getTag() == ListBufferTypeTag::Duration
        && element.getAs<types::Duration::Primitive>() == value;
}

bool db::operator==(const ListElementView element, const types::Embedding::Primitive value) {
    return element.getTag() == ListBufferTypeTag::Embedding
        && EmbeddingEqual {}(element.getAs<types::Embedding::Primitive>(), value);
}

bool db::operator==(const ListElementView element, const NodeID value) {
    return element.getTag() == ListBufferTypeTag::NodeID
        && element.getAs<NodeID>() == value;
}

bool db::operator==(const ListElementView element, const EdgeID value) {
    return element.getTag() == ListBufferTypeTag::EdgeID
        && element.getAs<EdgeID>() == value;
}

bool db::operator==(const ListElementView element, const ListView value) {
    return element.getTag() == ListBufferTypeTag::ListView
        && element.getAs<ListView>() == value;
}

bool db::operator==(const ListElementView element, const MapView value) {
    return element.getTag() == ListBufferTypeTag::MapView
        && element.getAs<MapView>() == value;
}

bool db::operator==(const ListElementView element, PropertyNull) {
    return element.getTag() == ListBufferTypeTag::Null;
}

bool db::operator==(const MapEntryView entry, PropertyNull) {
    return entry.getValueTag() == MapBufferTypeTag::Null;
}

bool db::operator==(const MapEntryView lhs, const MapEntryView rhs) {
    return valuesEqual(lhs, rhs);
}

bool db::operator==(const MapEntryView entry, const ListElementView element) {
    return valuesEqual(entry, element);
}

bool db::operator==(const MapEntryView entry, const types::Int64::Primitive value) {
    return elementEqualsNumber(entry, value);
}

bool db::operator==(const MapEntryView entry, const types::UInt64::Primitive value) {
    return elementEqualsNumber(entry, value);
}

bool db::operator==(const MapEntryView entry, const types::Double::Primitive value) {
    return elementEqualsNumber(entry, value);
}

bool db::operator==(const MapEntryView entry, const types::String::Primitive value) {
    return entry.getValueTag() == MapBufferTypeTag::String
        && entry.getValueAs<types::String::Primitive>() == value;
}

bool db::operator==(const MapEntryView entry, const types::Bool::Primitive value) {
    return entry.getValueTag() == MapBufferTypeTag::Bool
        && static_cast<bool>(entry.getValueAs<types::Bool::Primitive>()) == static_cast<bool>(value);
}

bool db::operator==(const MapEntryView entry, const ListView value) {
    return entry.getValueTag() == MapBufferTypeTag::ListView
        && entry.getValueAs<ListView>() == value;
}

bool db::operator==(const MapEntryView entry, const MapView value) {
    return entry.getValueTag() == MapBufferTypeTag::MapView
        && entry.getValueAs<MapView>() == value;
}

std::strong_ordering db::operator<=>(const ListElementView element, const types::Int64::Primitive value) {
    return compareElementWithNumber(element, value);
}

std::strong_ordering db::operator<=>(const ListElementView element, const types::UInt64::Primitive value) {
    return compareElementWithNumber(element, value);
}

std::strong_ordering db::operator<=>(const ListElementView element, const types::Double::Primitive value) {
    return compareElementWithNumber(element, value);
}

std::strong_ordering db::operator<=>(const ListElementView element, const types::String::Primitive value) {
    const ListElementOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != ListElementOrderClass::String) {
        return elementClass <=> ListElementOrderClass::String;
    }

    return element.getAs<types::String::Primitive>() <=> value;
}

std::strong_ordering db::operator<=>(const ListElementView element, const types::Bool::Primitive value) {
    const ListElementOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != ListElementOrderClass::Bool) {
        return elementClass <=> ListElementOrderClass::Bool;
    }

    return static_cast<bool>(element.getAs<types::Bool::Primitive>()) <=> static_cast<bool>(value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView lhs, const ListElementView rhs) {
    const ListBufferTypeTag rhsTag = rhs.getTag();

    switch (rhsTag) {
        case ListBufferTypeTag::Int:
            return comparisonOrder(lhs, rhs.getAs<types::Int64::Primitive>());
        break;

        case ListBufferTypeTag::UInt:
            return comparisonOrder(lhs, rhs.getAs<types::UInt64::Primitive>());
        break;

        case ListBufferTypeTag::Double:
            return comparisonOrder(lhs, rhs.getAs<types::Double::Primitive>());
        break;

        case ListBufferTypeTag::String:
            return comparisonOrder(lhs, rhs.getAs<types::String::Primitive>());
        break;

        case ListBufferTypeTag::Bool:
            return comparisonOrder(lhs, rhs.getAs<types::Bool::Primitive>());
        break;

        case ListBufferTypeTag::DateTime:
            if (lhs.getTag() != rhsTag) {
                return std::nullopt;
            }

            return lhs.getAs<types::DateTime::Primitive>() <=> rhs.getAs<types::DateTime::Primitive>();
        break;

        case ListBufferTypeTag::Duration:
            if (lhs.getTag() != rhsTag) {
                return std::nullopt;
            }

            return lhs.getAs<types::Duration::Primitive>() <=> rhs.getAs<types::Duration::Primitive>();
        break;

        case ListBufferTypeTag::ListView:
            if (lhs.getTag() != rhsTag) {
                return std::nullopt;
            }

            return comparisonOrder(lhs.getAs<ListView>(), rhs.getAs<ListView>());
        break;

        default:
            return std::nullopt;
        break;
    }
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListView lhs, const ListView rhs) {
    const std::span<const ListElementView> lhsElements = lhs.elements();
    const std::span<const ListElementView> rhsElements = rhs.elements();
    const size_t common = std::min(lhsElements.size(), rhsElements.size());

    for (size_t index = 0; index < common; index++) {
        const std::optional<std::partial_ordering> order = comparisonOrder(lhsElements[index], rhsElements[index]);
        if (!order || *order != std::partial_ordering::equivalent) {
            return order;
        }
    }

    return lhsElements.size() <=> rhsElements.size();
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView element, const types::Int64::Primitive value) {
    return elementNumberComparisonOrder(element, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView element, const types::UInt64::Primitive value) {
    return elementNumberComparisonOrder(element, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView element, const types::Double::Primitive value) {
    return elementNumberComparisonOrder(element, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView element, const types::String::Primitive value) {
    if (element.getTag() != ListBufferTypeTag::String) {
        return std::nullopt;
    }

    return element.getAs<types::String::Primitive>() <=> value;
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView element, const types::Bool::Primitive value) {
    if (element.getTag() != ListBufferTypeTag::Bool) {
        return std::nullopt;
    }

    return static_cast<bool>(element.getAs<types::Bool::Primitive>()) <=> static_cast<bool>(value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView element, const types::DateTime::Primitive value) {
    if (element.getTag() != ListBufferTypeTag::DateTime) {
        return std::nullopt;
    }

    return element.getAs<types::DateTime::Primitive>() <=> value;
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView element, const types::Duration::Primitive value) {
    if (element.getTag() != ListBufferTypeTag::Duration) {
        return std::nullopt;
    }

    return element.getAs<types::Duration::Primitive>() <=> value;
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListElementView element, const ListView value) {
    if (element.getTag() != ListBufferTypeTag::ListView) {
        return std::nullopt;
    }

    return comparisonOrder(element.getAs<ListView>(), value);
}

std::strong_ordering db::operator<=>(const ListElementView element, const types::DateTime::Primitive value) {
    const ListElementOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != ListElementOrderClass::DateTime) {
        return elementClass <=> ListElementOrderClass::DateTime;
    }

    return element.getAs<types::DateTime::Primitive>() <=> value;
}

std::strong_ordering db::operator<=>(const ListElementView element, const types::Duration::Primitive value) {
    const ListElementOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != ListElementOrderClass::Duration) {
        return elementClass <=> ListElementOrderClass::Duration;
    }

    return element.getAs<types::Duration::Primitive>() <=> value;
}
