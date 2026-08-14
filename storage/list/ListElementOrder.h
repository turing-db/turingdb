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
 * @brief Compares an element of a @ref ListByteBuffer against a value of a known type.
 *
 * Equal only when the element holds that value: a number compares numerically whatever
 * it is tagged as, while an element of another type - a null or a nested list included -
 * equals no scalar.
 */
bool operator==(ListElementView element, types::Int64::Primitive value);
bool operator==(ListElementView element, types::UInt64::Primitive value);
bool operator==(ListElementView element, types::Double::Primitive value);
bool operator==(ListElementView element, types::String::Primitive value);
bool operator==(ListElementView element, types::Bool::Primitive value);

}
