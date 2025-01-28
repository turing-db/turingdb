#pragma once

#include <concepts>

namespace db {

struct PropertyType;

template <typename T>
concept SupportedType = std::derived_from<T, PropertyType>;


}
