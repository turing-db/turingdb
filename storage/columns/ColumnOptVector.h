#pragma once

#include <optional>
#include <type_traits>

#include "ColumnVector.h"
#include "OptionalTraits.h"

namespace db {

template <typename T>
using ColumnOptVector = ColumnVector<std::optional<T>>;

template <typename T>
using ColumnVectorOf = std::conditional_t<OptionalTraits::is_optional_v<T>,
                                          ColumnOptVector<OptionalTraits::unwrap_optional_t<T>>,
                                          ColumnVector<T>>;

}
