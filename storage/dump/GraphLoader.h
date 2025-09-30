#pragma once

#include "Path.h"
#include "DumpResult.h"

namespace db {

class Graph;

class GraphLoader {
public:
    [[nodiscard]] static DumpResult<void> load(Graph* graph, const fs::Path& path);
};

}
