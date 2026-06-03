#pragma once

#include <array>
#include <string_view>

#include "Path.h"

namespace db {

// Commit-directory Parquet file layout: paths, column names and metadata keys of the
// commit-level files. The commit-level dumpers and loaders build their paths and
// schemas through here so the two sides cannot drift — the per-commit analogue of
// DataPartParquetLayout.
namespace commitParquetLayout {

inline fs::Path labels(const fs::Path& dir) { return dir / "labels.parquet"; }
inline fs::Path edgeTypes(const fs::Path& dir) { return dir / "edge-types.parquet"; }
inline fs::Path propertyTypes(const fs::Path& dir) { return dir / "property-types.parquet"; }
inline fs::Path labelsets(const fs::Path& dir) { return dir / "labelsets.parquet"; }
inline fs::Path journalNodes(const fs::Path& dir) { return dir / "journal-nodes.parquet"; }
inline fs::Path journalEdges(const fs::Path& dir) { return dir / "journal-edges.parquet"; }
inline fs::Path tombstoneNodes(const fs::Path& dir) { return dir / "tombstone-nodes.parquet"; }
inline fs::Path tombstoneEdges(const fs::Path& dir) { return dir / "tombstone-edges.parquet"; }
inline fs::Path commitMetaData(const fs::Path& dir) { return dir / "commit-metadata.parquet"; }

// journal-* / tombstone-* files: one id column each.
constexpr std::string_view NODE_ID_COLUMN = "node_id";
constexpr std::string_view EDGE_ID_COLUMN = "edge_id";

// commit-metadata.parquet: the datapart id column plus the commit scalars.
constexpr std::string_view DATA_PART_ID_COLUMN = "data_part_id";
constexpr std::string_view NUM_NODES_KEY = "turing.num_nodes";
constexpr std::string_view NUM_EDGES_KEY = "turing.num_edges";
constexpr std::string_view NUM_COMMIT_DATAPARTS_KEY = "turing.num_commit_dataparts";

// labels / edge-types / property-types / labelsets metadata tables.
constexpr std::string_view LABEL_ID_COLUMN = "label_id";
constexpr std::string_view EDGE_TYPE_ID_COLUMN = "edge_type_id";
constexpr std::string_view PROPERTY_TYPE_ID_COLUMN = "property_type_id";
constexpr std::string_view LABELSET_ID_COLUMN = "labelset_id";
constexpr std::string_view NAME_COLUMN = "name";
constexpr std::string_view VALUE_TYPE_COLUMN = "value_type";

constexpr std::array<std::string_view, 4> LABELSET_INTEGER_COLUMNS = {
    "integer_0", "integer_1", "integer_2", "integer_3"};

}

}
