#pragma once

#include <compare>
#include <optional>

#include "ID.h"
#include "CellView.h"
#include "list/ListView.h"

#include "map/MapView.h"

#include "metadata/PropertyType.h"

namespace db {

/**
 * @brief Orders two elements of a @ref ListByteBuffer, which need not share a type.
 *
 * Follows Cypher's orderability across types - MAP < NODE < EDGE < LIST < DATETIME < DURATION <
 * STRING < BOOLEAN < NUMBER < NULL - so a null sorts after every value and two elements of one type compare
 * by their own order: entities by their ID, numbers numerically whatever they are tagged
 * as, strings lexicographically, lists element-wise. An embedding has no order, so <=>
 * throws on one; == still compares embeddings element-wise, at any depth.
 */
std::strong_ordering operator<=>(CellView lhs, CellView rhs);
bool operator==(CellView lhs, CellView rhs);

/**
 * @brief Orders two lists of a @ref ListByteBuffer lexicographically.
 *
 * Compares pairwise from the front on the element order above, then puts the shorter
 * list first when one is a prefix of the other - Cypher's order over lists, and the
 * order a nested list element is compared on.
 */
std::strong_ordering operator<=>(ListView lhs, ListView rhs);

/**
 * @brief Orders two maps as Cypher does: the map with fewer entries first, then maps of one
 * size by their sorted keys, then maps of one key set by their values pairwise. Entries are
 * read in the order the maps hold them, which is sorted by key for every map codegen or a
 * MapContainer builds.
 */
std::strong_ordering operator<=>(MapView lhs, MapView rhs);

/**
 * @brief Cypher's equality of two cells, lists or maps, at any depth: unknown where a null
 * meets a value and no other pair differs. == is equivalence instead, under which two nulls
 * agree, which is what DISTINCT and grouping key on.
 */
std::optional<bool> equalOrUnknown(CellView lhs, CellView rhs);
std::optional<bool> equalOrUnknown(ListView lhs, ListView rhs);
std::optional<bool> equalOrUnknown(MapView lhs, MapView rhs);

/**
 * @brief Compares an element of a @ref ListByteBuffer against a value of a known type.
 *
 * Equal only when the element holds that value: a number compares numerically whatever
 * it is tagged as, a nested list element-wise against a list, a map entry-wise against a
 * map, and an element of any other type - a null included - equals neither.
 */
bool operator==(CellView element, types::Int64::Primitive value);
bool operator==(CellView element, types::UInt64::Primitive value);
bool operator==(CellView element, types::Double::Primitive value);
bool operator==(CellView element, types::String::Primitive value);
bool operator==(CellView element, types::Bool::Primitive value);
bool operator==(CellView element, types::DateTime::Primitive value);
bool operator==(CellView element, types::Duration::Primitive value);
bool operator==(CellView element, types::Embedding::Primitive value);
bool operator==(CellView element, NodeID value);
bool operator==(CellView element, EdgeID value);
bool operator==(CellView element, ListView value);
bool operator==(CellView element, MapView value);

/**
 * @brief Orders an element of a @ref ListByteBuffer against a value of a known type.
 *
 * The value is ordered as an element holding it would be, so the cross-type order above
 * settles a pair of different types - a string element sorts before any number, whichever
 * way round the query wrote the two - and a pair of one type compares in its own order.
 */
std::strong_ordering operator<=>(CellView element, types::Int64::Primitive value);
std::strong_ordering operator<=>(CellView element, types::UInt64::Primitive value);
std::strong_ordering operator<=>(CellView element, types::Double::Primitive value);
std::strong_ordering operator<=>(CellView element, types::String::Primitive value);
std::strong_ordering operator<=>(CellView element, types::Bool::Primitive value);
std::strong_ordering operator<=>(CellView element, types::DateTime::Primitive value);
std::strong_ordering operator<=>(CellView element, types::Duration::Primitive value);

/**
 * @brief Compares an element of a @ref ListByteBuffer as <, <=, > and >= do. Unlike the order
 * above, only two numbers, strings, booleans, datetimes, durations or lists compare: any other
 * pair, a null on either side included, is absent. A NaN is unordered against every number.
 */
std::optional<std::partial_ordering> comparisonOrder(CellView lhs, CellView rhs);
std::optional<std::partial_ordering> comparisonOrder(ListView lhs, ListView rhs);
std::optional<std::partial_ordering> comparisonOrder(CellView element, ListView value);
std::optional<std::partial_ordering> comparisonOrder(CellView element, types::Int64::Primitive value);
std::optional<std::partial_ordering> comparisonOrder(CellView element, types::UInt64::Primitive value);
std::optional<std::partial_ordering> comparisonOrder(CellView element, types::Double::Primitive value);
std::optional<std::partial_ordering> comparisonOrder(CellView element, types::String::Primitive value);
std::optional<std::partial_ordering> comparisonOrder(CellView element, types::Bool::Primitive value);
std::optional<std::partial_ordering> comparisonOrder(CellView element, types::DateTime::Primitive value);
std::optional<std::partial_ordering> comparisonOrder(CellView element, types::Duration::Primitive value);

}
