#pragma once

#include <expected.hpp>

template <class TValue, class TError>
using BasicResult = nonstd::expected<TValue, TError>;

template <class TError>
using BadResult = nonstd::unexpected_type<TError>;
