#include "ListElementOrder.h"

#include <stdint.h>

#include <algorithm>
#include <cmath>
#include <type_traits>
#include <utility>

#include "ID.h"
#include "ListBufferTypeTag.h"

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
    String,
    Bool,
    Number,
    DateTime,
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

template <typename View>
double asDouble(const View value) {
    switch (tagOf(value)) {
        case ListBufferTypeTag::Int:
            return static_cast<double>(valueOf<types::Int64::Primitive>(value));
        break;

        case ListBufferTypeTag::UInt:
            return static_cast<double>(valueOf<types::UInt64::Primitive>(value));
        break;

        default:
            return valueOf<types::Double::Primitive>(value);
        break;
    }
}

// Orders two numbers read as doubles. A NaN sorts after every number, as Cypher orders it.
std::strong_ordering compareDoubles(const double lhs, const double rhs) {
    const bool lhsIsNaN = std::isnan(lhs);
    const bool rhsIsNaN = std::isnan(rhs);

    if (lhsIsNaN || rhsIsNaN) {
        if (lhsIsNaN && rhsIsNaN) {
            return std::strong_ordering::equal;
        }

        return lhsIsNaN ? std::strong_ordering::greater : std::strong_ordering::less;
    }

    if (lhs < rhs) {
        return std::strong_ordering::less;
    } else if (rhs < lhs) {
        return std::strong_ordering::greater;
    }

    return std::strong_ordering::equal;
}

// Two numbers compare numerically whatever they are tagged as. A pair of integers of one
// signedness compares in its own type, so neighbouring values above 2^53 keep their
// order; any other pair goes through double, the type holding both.
template <typename View>
std::strong_ordering compareNumbers(const View lhs, const View rhs) {
    const ListBufferTypeTag lhsTag = tagOf(lhs);
    const ListBufferTypeTag rhsTag = tagOf(rhs);

    if (lhsTag == ListBufferTypeTag::Int && rhsTag == ListBufferTypeTag::Int) {
        return valueOf<types::Int64::Primitive>(lhs) <=> valueOf<types::Int64::Primitive>(rhs);
    } else if (lhsTag == ListBufferTypeTag::UInt && rhsTag == ListBufferTypeTag::UInt) {
        return valueOf<types::UInt64::Primitive>(lhs) <=> valueOf<types::UInt64::Primitive>(rhs);
    }

    return compareDoubles(asDouble(lhs), asDouble(rhs));
}

// Orders two tagged values - two list elements, or the values of two map entries - on the
// cross-type order the header documents
template <typename View>
std::strong_ordering compareValues(const View lhs, const View rhs) {
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

        case ListElementOrderClass::Null:
            return std::strong_ordering::equal;
        break;
    }

    throw FatalException("Unknown list element order class.");
}

// Compares a stored number against a value of another numeric type. Two integers
// compare through std::cmp_equal, so a mixed-sign pair keeps its value; any pair
// involving a float compares as doubles.
template <typename Stored, typename Value>
bool numericEquals(const Stored stored, const Value value) {
    if constexpr (std::is_integral_v<Stored> && std::is_integral_v<Value>) {
        return std::cmp_equal(stored, value);
    } else {
        return static_cast<double>(stored) == static_cast<double>(value);
    }
}

template <typename Value>
bool elementEqualsNumber(const ListElementView element, const Value value) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return numericEquals(element.getAs<types::Int64::Primitive>(), value);
        break;

        case ListBufferTypeTag::UInt:
            return numericEquals(element.getAs<types::UInt64::Primitive>(), value);
        break;

        case ListBufferTypeTag::Double:
            return numericEquals(element.getAs<types::Double::Primitive>(), value);
        break;

        default:
            return false;
        break;
    }
}

// Orders a stored number against one of another numeric type, on the rules compareNumbers
// follows between two stored ones - a pair of integers compares exactly whatever their
// signedness, which std::cmp_less gives for the mixed pair too.
template <typename Stored, typename Value>
std::strong_ordering compareNumericValues(const Stored stored, const Value value) {
    if constexpr (std::is_integral_v<Stored> && std::is_integral_v<Value>) {
        if (std::cmp_less(stored, value)) {
            return std::strong_ordering::less;
        } else if (std::cmp_less(value, stored)) {
            return std::strong_ordering::greater;
        }

        return std::strong_ordering::equal;
    } else {
        return compareDoubles(static_cast<double>(stored), static_cast<double>(value));
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

    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return compareNumericValues(element.getAs<types::Int64::Primitive>(), value);
        break;

        case ListBufferTypeTag::UInt:
            return compareNumericValues(element.getAs<types::UInt64::Primitive>(), value);
        break;

        default:
            return compareNumericValues(element.getAs<types::Double::Primitive>(), value);
        break;
    }
}

// Embeddings have no order, so equality cannot be read off compareValues wherever one may
// sit - directly, or anywhere inside a nested list or map
template <typename View>
bool valuesEqual(const View lhs, const View rhs) {
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
