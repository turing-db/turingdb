#pragma once

#include <string_view>

namespace db {

// Column names shared by PropertyIndexerParquetDumper and PropertyIndexerParquetLoader,
// so the two sides cannot drift.
namespace propertyIndexerParquetLayout {

constexpr std::string_view PROPERTY_TYPE_ID_COLUMN = "property_type_id";
constexpr std::string_view LABELSET_ID_COLUMN = "labelset_id";
constexpr std::string_view OFFSET_COLUMN = "offset";
constexpr std::string_view COUNT_COLUMN = "count";

}

}
