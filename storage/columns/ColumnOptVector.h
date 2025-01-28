#pragma once

#include <optional>

#include "ColumnVector.h"

namespace db {

template <typename T>
using ColumnOptVector = ColumnVector<std::optional<T>>;

}
