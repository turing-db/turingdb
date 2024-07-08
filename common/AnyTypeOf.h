#pragma once

#include <concepts>

template <typename T, typename... U>
concept any_type_of = (std::same_as<T, U> || ...);
