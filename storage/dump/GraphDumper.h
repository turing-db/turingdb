#pragma once

#include "Path.h"
#include "DumpResult.h"

namespace db {

class Graph;

class GraphDumper {
public:
    [[nodiscard]] static DumpResult<void> dump(const Graph& graph, const fs::Path& path);

private:
    [[nodiscard]] static DumpResult<void> dumpMissingCommits(const Graph& graph, const fs::Path& path);
};

}
