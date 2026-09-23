#pragma once

#include <stddef.h>

#include "ListElementView.h"
#include "ListView.h"

namespace db {

/**
 * @brief Hashes a list by its elements, so two equal lists hash alike whichever buffer
 * built them - the hash counterpart of @ref operator==(ListView, ListView).
 */
size_t hashList(ListView list);
size_t hashListElement(ListElementView element);

/// Hashes a number by its value, so an integer and a double that compare equal hash alike
size_t hashNumber(double value);

}
