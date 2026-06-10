#pragma once

#include <string_view>

namespace db {

// Column names and metadata keys shared by PropertyContainerParquetDumper and
// PropertyContainerParquetLoader, so the two sides cannot drift.
namespace propertyContainerParquetLayout {

constexpr std::string_view ENTITY_ID_COLUMN = "entity_id";
constexpr std::string_view VALUE_COLUMN = "value";

constexpr std::string_view VALUE_TYPE_KEY = "turing.value_type";
constexpr std::string_view DIMENSION_KEY = "turing.embedding_dimension";

}

}
