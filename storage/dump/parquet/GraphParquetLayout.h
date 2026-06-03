#pragma once

#include <stdint.h>

#include <string>
#include <string_view>

#include "Path.h"

namespace db {

// Graph-directory Parquet file layout. The graph- and commit-level orchestrators build
// their paths through here so the two sides cannot drift — the graph-level analogue of
// DataPartParquetLayout / CommitParquetLayout.
namespace graphParquetLayout {

// Version of the whole dump layout, stamped into graph-info.parquet's key/value
// metadata. The loader refuses any other version rather than misreading the files;
// bump it whenever the layout changes incompatibly.
constexpr uint64_t FORMAT_VERSION = 1;
constexpr std::string_view FORMAT_VERSION_KEY = "turing.format_version";

// graph-info.parquet / commit-log.parquet column names, shared by GraphParquetDumper
// and GraphParquetLoader.
constexpr std::string_view GRAPH_ID_COLUMN = "graph_id";
constexpr std::string_view NAME_COLUMN = "name";
constexpr std::string_view COMMIT_HASH_COLUMN = "commit_hash";

inline fs::Path graphInfo(const fs::Path& graphDir) { return graphDir / "graph-info.parquet"; }
inline fs::Path commitLog(const fs::Path& graphDir) { return graphDir / "commit-log.parquet"; }
inline fs::Path commitsDir(const fs::Path& graphDir) { return graphDir / "commits"; }
inline fs::Path dataPartsDir(const fs::Path& graphDir) { return graphDir / "dataparts"; }

// Sibling directory a dump is written into before being renamed over the final graph
// directory; one left behind is a crashed partial dump and is safe to remove.
inline fs::Path dumpTempDir(const fs::Path& graphDir) {
    return fs::Path {graphDir.get() + ".dumping"};
}

inline fs::Path commitDir(const fs::Path& graphDir, uint64_t commitHash) {
    return commitsDir(graphDir) / std::to_string(commitHash);
}

inline fs::Path dataPartDir(const fs::Path& graphDir, uint64_t dataPartId) {
    return dataPartsDir(graphDir) / std::to_string(dataPartId);
}

}

}
