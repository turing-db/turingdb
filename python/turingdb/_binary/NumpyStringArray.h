#pragma once

#include <stddef.h>
#include <optional>
#include <span>
#include <string_view>

#include <nanobind/nanobind.h>

namespace pybindings {

namespace nb = nanobind;

// Builds a NumPy StringDType array, with None as its missing value. Strings are packed into
// NumPy's own string storage, so no Python object is made per row.
nb::object makeStringArray(std::span<const std::string_view> rows);
nb::object makeStringArray(std::span<const std::optional<std::string_view>> rows);
nb::object makeStringArray(const std::optional<std::string_view>& value, size_t rowCount);

}
