#pragma once

#include <compare>

#include "ListElementView.h"
#include "ListView.h"

namespace db {

/**
 * @brief Orders two elements of a @ref ListByteBuffer, which need not share a type.
 *
 * Follows Cypher's orderability across types - LIST < STRING < BOOLEAN < NUMBER < NULL -
 * so a null sorts after every value and two elements of one type compare by their own
 * order: numbers numerically whatever they are tagged as, strings lexicographically,
 * lists element-wise. An embedding has no order and throws.
 */
std::strong_ordering operator<=>(ListElementView lhs, ListElementView rhs);
bool operator==(ListElementView lhs, ListElementView rhs);

}
