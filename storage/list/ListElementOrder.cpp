#include "ListElementOrder.h"

#include <stdint.h>

#include <algorithm>
#include <cmath>
#include <type_traits>
#include <utility>

#include "ID.h"
#include "ListBufferTypeTag.h"

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

        case ListBufferTypeTag::Embedding:
            throw FatalException("Cannot order an embedding list element.");
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

// Two numbers compare numerically whatever they are tagged as. A pair of integers of one
// signedness compares in its own type, so neighbouring values above 2^53 keep their
// order; any other pair goes through double, the type holding both. A NaN sorts after
// every number, as Cypher orders it.
std::strong_ordering compareNumbers(const ListElementView lhs, const ListElementView rhs) {
    const ListBufferTypeTag lhsTag = lhs.getTag();
    const ListBufferTypeTag rhsTag = rhs.getTag();

    if (lhsTag == ListBufferTypeTag::Int && rhsTag == ListBufferTypeTag::Int) {
        return lhs.getAs<types::Int64::Primitive>() <=> rhs.getAs<types::Int64::Primitive>();
    } else if (lhsTag == ListBufferTypeTag::UInt && rhsTag == ListBufferTypeTag::UInt) {
        return lhs.getAs<types::UInt64::Primitive>() <=> rhs.getAs<types::UInt64::Primitive>();
    }

    const double lhsValue = asDouble(lhs);
    const double rhsValue = asDouble(rhs);

    const bool lhsIsNaN = std::isnan(lhsValue);
    const bool rhsIsNaN = std::isnan(rhsValue);

    if (lhsIsNaN || rhsIsNaN) {
        if (lhsIsNaN && rhsIsNaN) {
            return std::strong_ordering::equal;
        }

        return lhsIsNaN ? std::strong_ordering::greater : std::strong_ordering::less;
    }

    if (lhsValue < rhsValue) {
        return std::strong_ordering::less;
    } else if (rhsValue < lhsValue) {
        return std::strong_ordering::greater;
    }

    return std::strong_ordering::equal;
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

// Two lists compare lexicographically: pairwise from the front, then the shorter list
// first when one is a prefix of the other.
std::strong_ordering compareLists(const ListView lhs, const ListView rhs) {
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
            return compareLists(lhs.getAs<ListView>(), rhs.getAs<ListView>());
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

        case ListElementOrderClass::Null:
            return std::strong_ordering::equal;
        break;
    }

    throw FatalException("Unknown list element order class.");
}

bool db::operator==(const ListElementView lhs, const ListElementView rhs) {
    return (lhs <=> rhs) == std::strong_ordering::equal;
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
