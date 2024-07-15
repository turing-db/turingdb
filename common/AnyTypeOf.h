#pragma once

#include <concepts>

template <typename T, typename... U>
concept any_type_of = (std::is_same_v<T, U> || ...);
