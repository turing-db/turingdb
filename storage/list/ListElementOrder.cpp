#include "ListElementOrder.h"

#include <stdint.h>

#include <algorithm>
#include <cmath>
#include <type_traits>
#include <utility>

#include "ID.h"
#include "ListBufferTypeTag.h"

#include "map/MapEntryView.h"
#include "map/MapUtils.h"
#include "map/MapView.h"

#include "metadata/PropertyType.h"

#include "FatalException.h"

using namespace db;

namespace {

// The class a tagged element sorts in, ascending.
enum class ListElementOrderClass {
    Node = 0,
    Edge,
    List,
    String,
    Bool,
    Number,
    DateTime,
    Null,
};

std::strong_ordering compareDoubles(double lhs, double rhs);

bool mapsEqual(MapView lhs, MapView rhs);

// Two entries hold the same value when they were stored under the same tag and the values
// under it match. A map value recurses; a list value goes through list equality.
bool isNumericValueTag(const MapBufferTypeTag tag) {
    return tag == MapBufferTypeTag::Int || tag == MapBufferTypeTag::UInt
           || tag == MapBufferTypeTag::Double;
}

double mapValueAsDouble(const MapEntryView entry) {
    switch (entry.getValueTag()) {
        case MapBufferTypeTag::Int:
            return static_cast<double>(entry.getValueAs<types::Int64::Primitive>());
        break;

        case MapBufferTypeTag::UInt:
            return static_cast<double>(entry.getValueAs<types::UInt64::Primitive>());
        break;

        default:
            return entry.getValueAs<types::Double::Primitive>();
        break;
    }
}

bool mapValuesEqual(const MapEntryView lhs, const MapEntryView rhs) {
    const MapBufferTypeTag tag = lhs.getValueTag();
    const MapBufferTypeTag rhsTag = rhs.getValueTag();

    // A number equals a number whatever tag each was stored under, as two list elements do
    if (isNumericValueTag(tag) && isNumericValueTag(rhsTag)) {
        if (tag == MapBufferTypeTag::Int && rhsTag == MapBufferTypeTag::Int) {
            return lhs.getValueAs<types::Int64::Primitive>() == rhs.getValueAs<types::Int64::Primitive>();
        } else if (tag == MapBufferTypeTag::UInt && rhsTag == MapBufferTypeTag::UInt) {
            return lhs.getValueAs<types::UInt64::Primitive>() == rhs.getValueAs<types::UInt64::Primitive>();
        }

        return compareDoubles(mapValueAsDouble(lhs), mapValueAsDouble(rhs)) == std::strong_ordering::equal;
    }

    if (tag != rhsTag) {
        return false;
    }

    const auto equalAs = [&rhs]<typename T>(const MapEntryView lhsEntry) -> bool {
        if constexpr (std::same_as<T, MapView>) {
            return mapsEqual(lhsEntry.getValueAs<MapView>(), rhs.getValueAs<MapView>());
        } else if constexpr (std::same_as<T, ListView>) {
            return lhsEntry.getValueAs<ListView>() == rhs.getValueAs<ListView>();
        } else if constexpr (std::same_as<T, PropertyNull>) {
            return true;
        } else if constexpr (std::same_as<T, types::Bool::Primitive>) {
            return static_cast<bool>(lhsEntry.getValueAs<T>()) == static_cast<bool>(rhs.getValueAs<T>());
        } else if constexpr (std::same_as<T, types::Embedding::Primitive>) {
            return std::ranges::equal(lhsEntry.getValueAs<T>(), rhs.getValueAs<T>());
        } else {
            return lhsEntry.getValueAs<T>() == rhs.getValueAs<T>();
        }
    };

    return MapTagDispatcher {tag}.execute(equalAs, lhs);
}

// Both producers of a map - the constant path's DictionaryAttr and db.make_map's sorted
// keys - store entries in key order, so equal maps line up entry for entry.
bool mapsEqual(const MapView lhs, const MapView rhs) {
    const std::span<const MapEntryView> lhsEntries = lhs.entries();
    const std::span<const MapEntryView> rhsEntries = rhs.entries();
    if (lhsEntries.size() != rhsEntries.size()) {
        return false;
    }

    for (size_t index = 0; index < lhsEntries.size(); index++) {
        if (lhsEntries[index].getKey() != rhsEntries[index].getKey()) {
            return false;
        }

        if (!mapValuesEqual(lhsEntries[index], rhsEntries[index])) {
            return false;
        }
    }

    return true;
}

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
            throw FatalException("Cannot order a map list element.");
        break;

        case ListBufferTypeTag::INVALID:
            throw FatalException("Cannot order an untagged list element.");
        break;
    }

    throw FatalException("Unknown ListBufferTypeTag.");
}

double asDouble(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return static_cast<double>(element.getAs<types::Int64::Primitive>());
        break;

        case ListBufferTypeTag::UInt:
            return static_cast<double>(element.getAs<types::UInt64::Primitive>());
        break;

        default:
            return element.getAs<types::Double::Primitive>();
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
std::strong_ordering compareNumbers(const ListElementView lhs, const ListElementView rhs) {
    const ListBufferTypeTag lhsTag = lhs.getTag();
    const ListBufferTypeTag rhsTag = rhs.getTag();

    if (lhsTag == ListBufferTypeTag::Int && rhsTag == ListBufferTypeTag::Int) {
        return lhs.getAs<types::Int64::Primitive>() <=> rhs.getAs<types::Int64::Primitive>();
    } else if (lhsTag == ListBufferTypeTag::UInt && rhsTag == ListBufferTypeTag::UInt) {
        return lhs.getAs<types::UInt64::Primitive>() <=> rhs.getAs<types::UInt64::Primitive>();
    }

    return compareDoubles(asDouble(lhs), asDouble(rhs));
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

}

std::strong_ordering db::operator<=>(const ListElementView lhs, const ListElementView rhs) {
    const ListElementOrderClass lhsClass = orderClassOf(lhs.getTag());
    const ListElementOrderClass rhsClass = orderClassOf(rhs.getTag());

    if (lhsClass != rhsClass) {
        return lhsClass <=> rhsClass;
    }

    switch (lhsClass) {
        case ListElementOrderClass::Node:
            return lhs.getAs<NodeID>() <=> rhs.getAs<NodeID>();
        break;

        case ListElementOrderClass::Edge:
            return lhs.getAs<EdgeID>() <=> rhs.getAs<EdgeID>();
        break;

        case ListElementOrderClass::List:
            return lhs.getAs<ListView>() <=> rhs.getAs<ListView>();
        break;

        case ListElementOrderClass::String:
            return lhs.getAs<types::String::Primitive>() <=> rhs.getAs<types::String::Primitive>();
        break;

        case ListElementOrderClass::Bool:
            return static_cast<bool>(lhs.getAs<types::Bool::Primitive>())
                <=> static_cast<bool>(rhs.getAs<types::Bool::Primitive>());
        break;

        case ListElementOrderClass::Number:
            return compareNumbers(lhs, rhs);
        break;

        case ListElementOrderClass::DateTime:
            return lhs.getAs<types::DateTime::Primitive>() <=> rhs.getAs<types::DateTime::Primitive>();
        break;

        case ListElementOrderClass::Null:
            return std::strong_ordering::equal;
        break;
    }

    throw FatalException("Unknown list element order class.");
}

bool db::operator==(const ListElementView lhs, const ListElementView rhs) {
    const ListBufferTypeTag lhsTag = lhs.getTag();
    const ListBufferTypeTag rhsTag = rhs.getTag();

    // A map has no order here, so equality cannot ask <=>; two maps are still plainly equal
    // or not, entry by entry
    if (lhsTag == ListBufferTypeTag::MapView || rhsTag == ListBufferTypeTag::MapView) {
        if (lhsTag != rhsTag) {
            return false;
        }

        return mapsEqual(lhs.getAs<MapView>(), rhs.getAs<MapView>());
    }

    // A nested list may hold a map further down, so it compares pairwise rather than
    // through <=>, which would reach the ordering a map has none of
    if (lhsTag == ListBufferTypeTag::ListView && rhsTag == ListBufferTypeTag::ListView) {
        return lhs.getAs<ListView>() == rhs.getAs<ListView>();
    }

    return (lhs <=> rhs) == std::strong_ordering::equal;
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
        if (!(lhsElements[index] == rhsElements[index])) {
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
