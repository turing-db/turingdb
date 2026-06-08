#pragma once

#include "Path.h"
#include "DumpResult.h"

namespace db {

class Graph;

class GraphDumper {
public:
    [[nodiscard]] static DumpResult<void> dump(Graph* graph, const fs::Path& graphDir);

private:
    [[nodiscard]] static DumpResult<void> dumpMissingCommits(Graph* graph, const fs::Path& graphDir);
};

}
