#include "CellOrder.h"

#include <stdint.h>

#include <algorithm>
#include <cmath>
#include <type_traits>

#include "ID.h"
#include "GraphPath.h"
#include "PathView.h"
#include "CellTag.h"
#include "NumberOrder.h"

#include "map/MapEntryView.h"
#include "map/MapView.h"

#include "metadata/PropertyType.h"

#include "FatalException.h"

using namespace db;

namespace {

// The class a tagged element sorts in, ascending.
enum class CellOrderClass {
    Map = 0,
    Node,
    Edge,
    List,
    Path,
    DateTime,
    Duration,
    String,
    Bool,
    Number,
    Null,
};

CellOrderClass orderClassOf(CellTag tag) {
    switch (tag) {
        case CellTag::NodeID:
            return CellOrderClass::Node;
        break;

        case CellTag::EdgeID:
            return CellOrderClass::Edge;
        break;

        case CellTag::ListView:
            return CellOrderClass::List;
        break;

        case CellTag::String:
            return CellOrderClass::String;
        break;

        case CellTag::Bool:
            return CellOrderClass::Bool;
        break;

        case CellTag::Int:
        case CellTag::UInt:
        case CellTag::Double:
            return CellOrderClass::Number;
        break;

        case CellTag::Null:
            return CellOrderClass::Null;
        break;

        case CellTag::DateTime:
            return CellOrderClass::DateTime;
        break;

        case CellTag::Duration:
            return CellOrderClass::Duration;
        break;

        case CellTag::Embedding:
            throw FatalException("Cannot order an embedding list element.");
        break;

        case CellTag::MapView:
            return CellOrderClass::Map;
        break;

        case CellTag::Path:
            return CellOrderClass::Path;
        break;

        case CellTag::INVALID:
            throw FatalException("Cannot order an untagged list element.");
        break;
    }

    throw FatalException("Unknown CellTag.");
}

CellTag tagOf(const CellView element) {
    return element.getTag();
}

template <typename T>
T valueOf(const CellView element) {
    return element.getAs<T>();
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
        case CellTag::Int:
            return compareNumericValues(valueOf<types::Int64::Primitive>(view), value);
        break;

        case CellTag::UInt:
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
        case CellTag::Int:
            return compareNumberWithValue(lhs, valueOf<types::Int64::Primitive>(rhs));
        break;

        case CellTag::UInt:
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
    const CellOrderClass lhsClass = orderClassOf(tagOf(lhs));
    const CellOrderClass rhsClass = orderClassOf(tagOf(rhs));

    if (lhsClass != rhsClass) {
        return lhsClass <=> rhsClass;
    }

    switch (lhsClass) {
        case CellOrderClass::Map:
            return valueOf<MapView>(lhs) <=> valueOf<MapView>(rhs);
        break;

        case CellOrderClass::Node:
            return valueOf<NodeID>(lhs) <=> valueOf<NodeID>(rhs);
        break;

        case CellOrderClass::Edge:
            return valueOf<EdgeID>(lhs) <=> valueOf<EdgeID>(rhs);
        break;

        case CellOrderClass::List:
            return valueOf<ListView>(lhs) <=> valueOf<ListView>(rhs);
        break;

        case CellOrderClass::Path: {
            const std::span<const uint64_t> lhsIDs = valueOf<PathView>(lhs).getIDs();
            const std::span<const uint64_t> rhsIDs = valueOf<PathView>(rhs).getIDs();

            return std::lexicographical_compare_three_way(lhsIDs.begin(), lhsIDs.end(), rhsIDs.begin(), rhsIDs.end());
        }
        break;

        case CellOrderClass::String:
            return valueOf<types::String::Primitive>(lhs) <=> valueOf<types::String::Primitive>(rhs);
        break;

        case CellOrderClass::Bool:
            return static_cast<bool>(valueOf<types::Bool::Primitive>(lhs))
                <=> static_cast<bool>(valueOf<types::Bool::Primitive>(rhs));
        break;

        case CellOrderClass::Number:
            return compareNumbers(lhs, rhs);
        break;

        case CellOrderClass::DateTime:
            return valueOf<types::DateTime::Primitive>(lhs) <=> valueOf<types::DateTime::Primitive>(rhs);
        break;

        case CellOrderClass::Duration:
            return valueOf<types::Duration::Primitive>(lhs) <=> valueOf<types::Duration::Primitive>(rhs);
        break;

        case CellOrderClass::Null:
            return std::strong_ordering::equal;
        break;
    }

    throw FatalException("Unknown list element order class.");
}

template <typename View, typename Value>
bool elementEqualsNumber(const View view, const Value value) {
    switch (tagOf(view)) {
        case CellTag::Int:
            return numbersEqual(valueOf<types::Int64::Primitive>(view), value);
        break;

        case CellTag::UInt:
            return numbersEqual(valueOf<types::UInt64::Primitive>(view), value);
        break;

        case CellTag::Double:
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
std::strong_ordering compareElementWithNumber(const CellView element, const Value value) {
    const CellOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != CellOrderClass::Number) {
        return elementClass <=> CellOrderClass::Number;
    }

    return compareNumberWithValue(element, value);
}

// Embeddings have no order, so equality cannot be read off compareValues wherever one may
// sit - directly, or anywhere inside a nested list or map
template <typename Lhs, typename Rhs>
bool valuesEqual(const Lhs lhs, const Rhs rhs) {
    const CellTag lhsTag = tagOf(lhs);
    const CellTag rhsTag = tagOf(rhs);

    const bool lhsIsEmbedding = lhsTag == CellTag::Embedding;
    const bool rhsIsEmbedding = rhsTag == CellTag::Embedding;
    const bool bothMaps = lhsTag == CellTag::MapView && rhsTag == CellTag::MapView;
    const bool bothLists = lhsTag == CellTag::ListView && rhsTag == CellTag::ListView;

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

template <typename View, typename Value>
std::optional<std::partial_ordering> numberComparisonOrder(const View view, const Value value) {
    switch (tagOf(view)) {
        case CellTag::Int:
            return numberOrder(valueOf<types::Int64::Primitive>(view), value);
        break;

        case CellTag::UInt:
            return numberOrder(valueOf<types::UInt64::Primitive>(view), value);
        break;

        case CellTag::Double:
            return numberOrder(valueOf<types::Double::Primitive>(view), value);
        break;

        default:
            return std::nullopt;
        break;
    }
}

template <typename Value, typename View>
std::optional<std::partial_ordering> sameTypeComparisonOrder(const View view, const CellTag tag, const Value value) {
    if (tagOf(view) != tag) {
        return std::nullopt;
    }

    if constexpr (std::is_same_v<Value, types::Bool::Primitive>) {
        return static_cast<bool>(valueOf<Value>(view)) <=> static_cast<bool>(value);
    } else if constexpr (std::is_same_v<Value, ListView>) {
        return comparisonOrder(valueOf<ListView>(view), value);
    } else {
        return valueOf<Value>(view) <=> value;
    }
}

// Compares two tagged values - list elements or the values of map entries, in any mix - as
// <, <=, > and >= do
template <typename Lhs, typename Rhs>
std::optional<std::partial_ordering> taggedComparisonOrder(const Lhs lhs, const Rhs rhs) {
    const CellTag rhsTag = tagOf(rhs);

    switch (rhsTag) {
        case CellTag::Int:
            return numberComparisonOrder(lhs, valueOf<types::Int64::Primitive>(rhs));
        break;

        case CellTag::UInt:
            return numberComparisonOrder(lhs, valueOf<types::UInt64::Primitive>(rhs));
        break;

        case CellTag::Double:
            return numberComparisonOrder(lhs, valueOf<types::Double::Primitive>(rhs));
        break;

        case CellTag::String:
            return sameTypeComparisonOrder(lhs, rhsTag, valueOf<types::String::Primitive>(rhs));
        break;

        case CellTag::Bool:
            return sameTypeComparisonOrder(lhs, rhsTag, valueOf<types::Bool::Primitive>(rhs));
        break;

        case CellTag::DateTime:
            return sameTypeComparisonOrder(lhs, rhsTag, valueOf<types::DateTime::Primitive>(rhs));
        break;

        case CellTag::Duration:
            return sameTypeComparisonOrder(lhs, rhsTag, valueOf<types::Duration::Primitive>(rhs));
        break;

        case CellTag::ListView:
            return sameTypeComparisonOrder(lhs, rhsTag, valueOf<ListView>(rhs));
        break;

        default:
            return std::nullopt;
        break;
    }
}

}

std::strong_ordering db::operator<=>(const CellView lhs, const CellView rhs) {
    return compareValues(lhs, rhs);
}

bool db::operator==(const CellView lhs, const CellView rhs) {
    return valuesEqual(lhs, rhs);
}

std::strong_ordering db::operator<=>(const ListView lhs, const ListView rhs) {
    const std::span<const CellView> lhsElements = lhs.elements();
    const std::span<const CellView> rhsElements = rhs.elements();
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
    const std::span<const CellView> lhsElements = lhs.elements();
    const std::span<const CellView> rhsElements = rhs.elements();
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

    const std::strong_ordering sizeOrder = lhsEntries.size() <=> rhsEntries.size();
    if (sizeOrder != std::strong_ordering::equal) {
        return sizeOrder;
    }

    for (size_t index = 0; index < lhsEntries.size(); index++) {
        const std::strong_ordering keyOrder = lhsEntries[index].getKey() <=> rhsEntries[index].getKey();
        if (keyOrder != std::strong_ordering::equal) {
            return keyOrder;
        }
    }

    for (size_t index = 0; index < lhsEntries.size(); index++) {
        const std::strong_ordering valueOrder = compareValues(lhsEntries[index].getValue(), rhsEntries[index].getValue());
        if (valueOrder != std::strong_ordering::equal) {
            return valueOrder;
        }
    }

    return std::strong_ordering::equal;
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

        const bool sameEntry = lhsEntry.getKey() == rhsEntry.getKey() && valuesEqual(lhsEntry.getValue(), rhsEntry.getValue());
        if (!sameEntry) {
            return false;
        }
    }

    return true;
}

std::optional<bool> db::equalOrUnknown(const CellView lhs, const CellView rhs) {
    const CellTag lhsTag = tagOf(lhs);
    const CellTag rhsTag = tagOf(rhs);

    const bool holdsNull = lhsTag == CellTag::Null || rhsTag == CellTag::Null;
    const bool bothMaps = lhsTag == CellTag::MapView && rhsTag == CellTag::MapView;
    const bool bothLists = lhsTag == CellTag::ListView && rhsTag == CellTag::ListView;

    if (holdsNull) {
        return std::nullopt;
    } else if (bothMaps) {
        return equalOrUnknown(valueOf<MapView>(lhs), valueOf<MapView>(rhs));
    } else if (bothLists) {
        return equalOrUnknown(valueOf<ListView>(lhs), valueOf<ListView>(rhs));
    }

    return valuesEqual(lhs, rhs);
}

std::optional<bool> db::equalOrUnknown(const ListView lhs, const ListView rhs) {
    const std::span<const CellView> lhsElements = lhs.elements();
    const std::span<const CellView> rhsElements = rhs.elements();
    if (lhsElements.size() != rhsElements.size()) {
        return false;
    }

    bool unknown = false;

    for (size_t index = 0; index < lhsElements.size(); index++) {
        const std::optional<bool> equal = equalOrUnknown(lhsElements[index], rhsElements[index]);
        if (!equal.has_value()) {
            unknown = true;
        } else if (!*equal) {
            return false;
        }
    }

    if (unknown) {
        return std::nullopt;
    }

    return true;
}

std::optional<bool> db::equalOrUnknown(const MapView lhs, const MapView rhs) {
    const std::span<const MapEntryView> lhsEntries = lhs.entries();
    const std::span<const MapEntryView> rhsEntries = rhs.entries();
    if (lhsEntries.size() != rhsEntries.size()) {
        return false;
    }

    bool unknown = false;

    for (size_t index = 0; index < lhsEntries.size(); index++) {
        const MapEntryView lhsEntry = lhsEntries[index];
        const MapEntryView rhsEntry = rhsEntries[index];
        if (lhsEntry.getKey() != rhsEntry.getKey()) {
            return false;
        }

        const std::optional<bool> equal = equalOrUnknown(lhsEntry.getValue(), rhsEntry.getValue());
        if (!equal.has_value()) {
            unknown = true;
        } else if (!*equal) {
            return false;
        }
    }

    if (unknown) {
        return std::nullopt;
    }

    return true;
}

bool db::operator==(const CellView element, const types::Int64::Primitive value) {
    return elementEqualsNumber(element, value);
}

bool db::operator==(const CellView element, const types::UInt64::Primitive value) {
    return elementEqualsNumber(element, value);
}

bool db::operator==(const CellView element, const types::Double::Primitive value) {
    return elementEqualsNumber(element, value);
}

bool db::operator==(const CellView element, const types::String::Primitive value) {
    return element.getTag() == CellTag::String
        && element.getAs<types::String::Primitive>() == value;
}

bool db::operator==(const CellView element, const types::Bool::Primitive value) {
    return element.getTag() == CellTag::Bool
        && static_cast<bool>(element.getAs<types::Bool::Primitive>()) == static_cast<bool>(value);
}

bool db::operator==(const CellView element, const types::DateTime::Primitive value) {
    return element.getTag() == CellTag::DateTime
        && element.getAs<types::DateTime::Primitive>() == value;
}

bool db::operator==(const CellView element, const types::Duration::Primitive value) {
    return element.getTag() == CellTag::Duration
        && element.getAs<types::Duration::Primitive>() == value;
}

bool db::operator==(const CellView element, const types::Embedding::Primitive value) {
    return element.getTag() == CellTag::Embedding
        && EmbeddingEqual {}(element.getAs<types::Embedding::Primitive>(), value);
}

bool db::operator==(const CellView element, const NodeID value) {
    return element.getTag() == CellTag::NodeID
        && element.getAs<NodeID>() == value;
}

bool db::operator==(const CellView element, const EdgeID value) {
    return element.getTag() == CellTag::EdgeID
        && element.getAs<EdgeID>() == value;
}

bool db::operator==(const CellView element, const ListView value) {
    return element.getTag() == CellTag::ListView
        && element.getAs<ListView>() == value;
}

bool db::operator==(const CellView element, const MapView value) {
    return element.getTag() == CellTag::MapView
        && element.getAs<MapView>() == value;
}

bool db::operator==(const CellView element, const PathView value) {
    return element.getTag() == CellTag::Path
        && std::ranges::equal(element.getAs<PathView>().getIDs(), value.getIDs());
}

bool db::operator==(const CellView element, const GraphPath& value) {
    return element == PathView {value.getIDs()};
}

std::strong_ordering db::operator<=>(const CellView element, const types::Int64::Primitive value) {
    return compareElementWithNumber(element, value);
}

std::strong_ordering db::operator<=>(const CellView element, const types::UInt64::Primitive value) {
    return compareElementWithNumber(element, value);
}

std::strong_ordering db::operator<=>(const CellView element, const types::Double::Primitive value) {
    return compareElementWithNumber(element, value);
}

std::strong_ordering db::operator<=>(const CellView element, const types::String::Primitive value) {
    const CellOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != CellOrderClass::String) {
        return elementClass <=> CellOrderClass::String;
    }

    return element.getAs<types::String::Primitive>() <=> value;
}

std::strong_ordering db::operator<=>(const CellView element, const types::Bool::Primitive value) {
    const CellOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != CellOrderClass::Bool) {
        return elementClass <=> CellOrderClass::Bool;
    }

    return static_cast<bool>(element.getAs<types::Bool::Primitive>()) <=> static_cast<bool>(value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView lhs, const CellView rhs) {
    return taggedComparisonOrder(lhs, rhs);
}

std::optional<std::partial_ordering> db::comparisonOrder(const ListView lhs, const ListView rhs) {
    const std::span<const CellView> lhsElements = lhs.elements();
    const std::span<const CellView> rhsElements = rhs.elements();
    const size_t common = std::min(lhsElements.size(), rhsElements.size());

    for (size_t index = 0; index < common; index++) {
        const std::optional<std::partial_ordering> order = comparisonOrder(lhsElements[index], rhsElements[index]);
        if (!order || *order != std::partial_ordering::equivalent) {
            return order;
        }
    }

    return lhsElements.size() <=> rhsElements.size();
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView element, const types::Int64::Primitive value) {
    return numberComparisonOrder(element, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView element, const types::UInt64::Primitive value) {
    return numberComparisonOrder(element, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView element, const types::Double::Primitive value) {
    return numberComparisonOrder(element, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView element, const types::String::Primitive value) {
    return sameTypeComparisonOrder(element, CellTag::String, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView element, const types::Bool::Primitive value) {
    return sameTypeComparisonOrder(element, CellTag::Bool, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView element, const types::DateTime::Primitive value) {
    return sameTypeComparisonOrder(element, CellTag::DateTime, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView element, const types::Duration::Primitive value) {
    return sameTypeComparisonOrder(element, CellTag::Duration, value);
}

std::optional<std::partial_ordering> db::comparisonOrder(const CellView element, const ListView value) {
    return sameTypeComparisonOrder(element, CellTag::ListView, value);
}

std::strong_ordering db::operator<=>(const CellView element, const types::DateTime::Primitive value) {
    const CellOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != CellOrderClass::DateTime) {
        return elementClass <=> CellOrderClass::DateTime;
    }

    return element.getAs<types::DateTime::Primitive>() <=> value;
}

std::strong_ordering db::operator<=>(const CellView element, const types::Duration::Primitive value) {
    const CellOrderClass elementClass = orderClassOf(element.getTag());
    if (elementClass != CellOrderClass::Duration) {
        return elementClass <=> CellOrderClass::Duration;
    }

    return element.getAs<types::Duration::Primitive>() <=> value;
}
