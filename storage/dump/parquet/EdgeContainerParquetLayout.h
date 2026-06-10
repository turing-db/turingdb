#pragma once

#include <string_view>

namespace db {

// Column names and metadata keys shared by EdgeContainerParquetDumper and
// EdgeContainerParquetLoader, so the two sides cannot drift.
namespace edgeContainerParquetLayout {

constexpr std::string_view EDGE_ID_COLUMN = "edge_id";
constexpr std::string_view NODE_ID_COLUMN = "node_id";
constexpr std::string_view OTHER_ID_COLUMN = "other_id";
constexpr std::string_view EDGE_TYPE_ID_COLUMN = "edge_type_id";

constexpr std::string_view FIRST_EDGE_ID_KEY = "turing.first_edge_id";
constexpr std::string_view FIRST_NODE_ID_KEY = "turing.first_node_id";

}

}
