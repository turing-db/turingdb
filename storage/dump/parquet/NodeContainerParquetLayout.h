#pragma once

#include <string_view>

namespace db {

// Column names and metadata keys shared by NodeContainerParquetDumper and
// NodeContainerParquetLoader, so the two sides cannot drift.
namespace nodeContainerParquetLayout {

constexpr std::string_view LABELSET_ID_COLUMN = "labelset_id";
constexpr std::string_view FIRST_NODE_ID_COLUMN = "first_node_id";
constexpr std::string_view COUNT_COLUMN = "count";

constexpr std::string_view FIRST_NODE_ID_KEY = "turing.first_node_id";

}

}
