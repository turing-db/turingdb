#pragma once

#include "Path.h"

namespace db {

// Commit-directory Parquet file layout. The commit-level dumpers and loaders build
// their paths through here so the two sides cannot drift — the per-commit analogue of
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

}

}
