#pragma once

#include <compare>

#include "ListElementView.h"
#include "ListView.h"

#include "map/MapView.h"

#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

namespace db {

/**
 * @brief Orders two elements of a @ref ListByteBuffer, which need not share a type.
 *
 * Follows Cypher's orderability across types - MAP < NODE < EDGE < LIST < STRING < BOOLEAN <
 * NUMBER < NULL - so a null sorts after every value and two elements of one type compare
 * by their own order: entities by their ID, numbers numerically whatever they are tagged
 * as, strings lexicographically, lists element-wise. An embedding has no order, so <=>
 * throws on one; == still compares embeddings element-wise, at any depth.
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
 * @brief Orders two maps entry by entry, on the key and then on the value, putting the
 * shorter map first when one is a prefix of the other. Entries are compared in the order
 * the maps hold them, which is sorted by key for every map codegen or a MapContainer builds.
 */
std::strong_ordering operator<=>(MapView lhs, MapView rhs);
bool operator==(MapView lhs, MapView rhs);

/**
 * @brief Compares an element of a @ref ListByteBuffer against a value of a known type.
 *
 * Equal only when the element holds that value: a number compares numerically whatever
 * it is tagged as, a nested list element-wise against a list, a map entry-wise against a
 * map, and an element of any other type - a null included - equals neither.
 */
bool operator==(ListElementView element, types::Int64::Primitive value);
bool operator==(ListElementView element, types::UInt64::Primitive value);
bool operator==(ListElementView element, types::Double::Primitive value);
bool operator==(ListElementView element, types::String::Primitive value);
bool operator==(ListElementView element, types::Bool::Primitive value);
bool operator==(ListElementView element, ListView value);
bool operator==(ListElementView element, MapView value);

/**
 * @brief Tests an element of a @ref ListByteBuffer for null, as IS (NOT) NULL does.
 *
 * A tagged cell carries its own null rather than riding a nullable column, so the tag is
 * what the test reads. Declared here so the test resolves to it rather than to
 * PropertyNull.h's fallback, which answers false for any type it knows nothing about.
 */
bool operator==(ListElementView element, PropertyNull);

/**
 * @brief Orders an element of a @ref ListByteBuffer against a value of a known type.
 *
 * The value is ordered as an element holding it would be, so the cross-type order above
 * settles a pair of different types - a string element sorts before any number, whichever
 * way round the query wrote the two - and a pair of one type compares in its own order.
 */
std::strong_ordering operator<=>(ListElementView element, types::Int64::Primitive value);
std::strong_ordering operator<=>(ListElementView element, types::UInt64::Primitive value);
std::strong_ordering operator<=>(ListElementView element, types::Double::Primitive value);
std::strong_ordering operator<=>(ListElementView element, types::String::Primitive value);
std::strong_ordering operator<=>(ListElementView element, types::Bool::Primitive value);

}
