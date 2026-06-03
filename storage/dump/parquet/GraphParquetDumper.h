#pragma once

#include "Path.h"

namespace db {

class Graph;

// Serializes a whole graph to a Parquet graph directory: a graph-info file (graph id and
// name), a commit-log file (the commit hashes in commit order), one Parquet commit
// directory per commit (CommitParquetDumper), and the shared dataparts directory. Mirrors
// the binary GraphDumper but uses only the graph's public accessors. Throws on failure.
class GraphParquetDumper {
public:
    static void dump(const Graph& graph, const fs::Path& graphDir);
};

}
