#pragma once

#include <compare>

#include "ListElementView.h"
#include "ListView.h"

#include "metadata/PropertyType.h"

namespace db {

/**
 * @brief Orders two elements of a @ref ListByteBuffer, which need not share a type.
 *
 * Follows Cypher's orderability across types - NODE < EDGE < LIST < STRING < BOOLEAN <
 * NUMBER < NULL - so a null sorts after every value and two elements of one type compare
 * by their own order: entities by their ID, numbers numerically whatever they are tagged
 * as, strings lexicographically, lists element-wise. An embedding has no order and throws.
 */
std::strong_ordering operator<=>(ListElementView lhs, ListElementView rhs);
bool operator==(ListElementView lhs, ListElementView rhs);

/**
 * @brief Orders two lists of a @ref ListByteBuffer lexicographically.
 *
 * Compares pairwise from the front on the element order above, then puts the shorter
 * list first when one is a prefix of the other - Cypher's order over lists, and the
 * order a nested list element is compared on.
 */
std::strong_ordering operator<=>(ListView lhs, ListView rhs);
bool operator==(ListView lhs, ListView rhs);

/**
 * @brief Compares an element of a @ref ListByteBuffer against a value of a known type.
 *
 * Equal only when the element holds that value: a number compares numerically whatever
 * it is tagged as, a nested list element-wise against a list, and an element of any other
 * type - a null included - equals neither.
 */
bool operator==(ListElementView element, types::Int64::Primitive value);
bool operator==(ListElementView element, types::UInt64::Primitive value);
bool operator==(ListElementView element, types::Double::Primitive value);
bool operator==(ListElementView element, types::String::Primitive value);
bool operator==(ListElementView element, types::Bool::Primitive value);
bool operator==(ListElementView element, ListView value);

}
