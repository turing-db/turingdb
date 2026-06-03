#pragma once

#include <stdint.h>

#include <string>

#include "Path.h"

namespace db {

// Graph-directory Parquet file layout. The graph- and commit-level orchestrators build
// their paths through here so the two sides cannot drift — the graph-level analogue of
// DataPartParquetLayout / CommitParquetLayout.
namespace graphParquetLayout {

inline fs::Path graphInfo(const fs::Path& graphDir) { return graphDir / "graph-info.parquet"; }
inline fs::Path commitLog(const fs::Path& graphDir) { return graphDir / "commit-log.parquet"; }
inline fs::Path commitsDir(const fs::Path& graphDir) { return graphDir / "commits"; }
inline fs::Path dataPartsDir(const fs::Path& graphDir) { return graphDir / "dataparts"; }

inline fs::Path commitDir(const fs::Path& graphDir, uint64_t commitHash) {
    return commitsDir(graphDir) / std::to_string(commitHash);
}

inline fs::Path dataPartDir(const fs::Path& graphDir, uint64_t dataPartId) {
    return dataPartsDir(graphDir) / std::to_string(dataPartId);
}

}

}
