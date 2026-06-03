#pragma once

#include <stdint.h>

#include <string>
#include <string_view>

#include "Path.h"

namespace db {

// Per-DataPart Parquet file layout. The DataPart orchestrator owns these names;
// both the dumper and the loader build paths through here so they cannot drift.
namespace dataPartParquetLayout {

constexpr std::string_view NODE_PROPS_PREFIX = "node-props-";
constexpr std::string_view EDGE_PROPS_PREFIX = "edge-props-";
constexpr std::string_view PARQUET_SUFFIX = ".parquet";

inline fs::Path info(const fs::Path& dir) { return dir / "info.parquet"; }
inline fs::Path nodeRanges(const fs::Path& dir) { return dir / "node-ranges.parquet"; }
inline fs::Path nodeRecords(const fs::Path& dir) { return dir / "node-records.parquet"; }
inline fs::Path edgesOut(const fs::Path& dir) { return dir / "edges-out.parquet"; }
inline fs::Path edgesIn(const fs::Path& dir) { return dir / "edges-in.parquet"; }
inline fs::Path edgeIndexerNodeData(const fs::Path& dir) { return dir / "edge-indexer-nodedata.parquet"; }
inline fs::Path edgeIndexerOutSpans(const fs::Path& dir) { return dir / "edge-indexer-out-spans.parquet"; }
inline fs::Path edgeIndexerInSpans(const fs::Path& dir) { return dir / "edge-indexer-in-spans.parquet"; }
inline fs::Path nodePropIndexer(const fs::Path& dir) { return dir / "node-prop-indexer.parquet"; }
inline fs::Path edgePropIndexer(const fs::Path& dir) { return dir / "edge-prop-indexer.parquet"; }

inline fs::Path nodeProps(const fs::Path& dir, uint64_t propertyTypeID) {
    return dir / (std::string(NODE_PROPS_PREFIX) + std::to_string(propertyTypeID)
                  + std::string(PARQUET_SUFFIX));
}

inline fs::Path edgeProps(const fs::Path& dir, uint64_t propertyTypeID) {
    return dir / (std::string(EDGE_PROPS_PREFIX) + std::to_string(propertyTypeID)
                  + std::string(PARQUET_SUFFIX));
}

inline fs::Path nodeStringIndexes(const fs::Path& dir) { return dir / "node-string-index-indexes.parquet"; }
inline fs::Path nodeStringChildren(const fs::Path& dir) { return dir / "node-string-index-children.parquet"; }
inline fs::Path nodeStringOwners(const fs::Path& dir) { return dir / "node-string-index-owners.parquet"; }
inline fs::Path edgeStringIndexes(const fs::Path& dir) { return dir / "edge-string-index-indexes.parquet"; }
inline fs::Path edgeStringChildren(const fs::Path& dir) { return dir / "edge-string-index-children.parquet"; }
inline fs::Path edgeStringOwners(const fs::Path& dir) { return dir / "edge-string-index-owners.parquet"; }

}

}
