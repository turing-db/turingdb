#pragma once

// Force expected-lite to use nonstd::expected instead of std::expected
// to maintain compatibility with code using get_unexpected()
#define nsel_CONFIG_SELECT_EXPECTED 1

#include <expected.hpp>

template <class TValue, class TError>
using BasicResult = nonstd::expected<TValue, TError>;

template <class TError>
using BadResult = nonstd::unexpected_type<TError>;
