#pragma once

#include "DumpResult.h"
#include "Path.h"

namespace db {
class VersionController;
class Graph;
class Commit;

class DumpAndLoadManager {
public:
    explicit DumpAndLoadManager(Graph* graph);
    ~DumpAndLoadManager() = default;

    DumpResult<void> loadGraph() const;
    DumpResult<void> dumpGraph() const;

    DumpResult<void> dumpCommit(const Commit& commit) const;

private:
    mutable std::mutex _mutex;

    Graph* _graph;
};

}
