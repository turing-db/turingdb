#pragma once

#include <string_view>

namespace db {

// Column names shared by StringPropertyIndexerParquetDumper and
// StringPropertyIndexerParquetLoader, so the two sides cannot drift.
namespace stringPropertyIndexerParquetLayout {

constexpr std::string_view PROPERTY_TYPE_ID_COLUMN = "property_type_id";
constexpr std::string_view NODE_COUNT_COLUMN = "node_count";
constexpr std::string_view PARENT_NODE_ID_COLUMN = "parent_node_id";
constexpr std::string_view CHILD_INDEX_COLUMN = "child_index";
constexpr std::string_view CHILD_NODE_ID_COLUMN = "child_node_id";
constexpr std::string_view NODE_ID_COLUMN = "node_id";
constexpr std::string_view ENTITY_ID_COLUMN = "entity_id";

}

}
