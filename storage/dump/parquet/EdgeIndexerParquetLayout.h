#pragma once

#include <string_view>

namespace db {

// Column names and metadata keys shared by EdgeIndexerParquetDumper and
// EdgeIndexerParquetLoader, so the two sides cannot drift.
namespace edgeIndexerParquetLayout {

constexpr std::string_view OUT_FIRST_COLUMN = "out_first";
constexpr std::string_view OUT_COUNT_COLUMN = "out_count";
constexpr std::string_view IN_FIRST_COLUMN = "in_first";
constexpr std::string_view IN_COUNT_COLUMN = "in_count";
constexpr std::string_view LABELSET_ID_COLUMN = "labelset_id";
constexpr std::string_view SPAN_OFFSET_COLUMN = "span_offset";
constexpr std::string_view SPAN_COUNT_COLUMN = "span_count";

constexpr std::string_view FIRST_NODE_ID_KEY = "turing.first_node_id";
constexpr std::string_view FIRST_EDGE_ID_KEY = "turing.first_edge_id";
constexpr std::string_view CORE_NODE_COUNT_KEY = "turing.core_node_count";
constexpr std::string_view PATCH_NODE_COUNT_KEY = "turing.patch_node_count";

}

}
