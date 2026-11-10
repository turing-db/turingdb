#pragma once

#include <stddef.h>

namespace db {

class Column;

// Appends rows [offset, offset + rowCount) of source onto destination, a column of its kind.
// A constant holds one value for every row and has no window to append: assign it instead.
void appendColumnRows(const Column* source, Column* destination, size_t offset, size_t rowCount);

}
