#pragma once

#include "Path.h"

namespace db {

class Graph;

// Rebuilds a whole graph from a Parquet graph directory written by GraphParquetDumper:
// reads the graph id and name, recreates the VersionController, loads every commit shell
// from the commit log (CommitParquetLoader::load), then materializes the head commit's
// full data (CommitParquetLoader::loadData) — only the head, matching the binary
// GraphLoader. Friend of Graph / VersionController to fill their private members. Throws
// on failure.
class GraphParquetLoader {
public:
    static void load(Graph* graph, const fs::Path& graphDir);
};

}
