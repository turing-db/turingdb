#pragma once

#include "DumpResult.h"

namespace db {

class VersionController;
class Graph;
class Commit;

class GraphSerializer {
public:
    explicit GraphSerializer(Graph* graph);
    ~GraphSerializer() = default;

    GraphSerializer(const GraphSerializer&) = delete;
    GraphSerializer(GraphSerializer&&) = delete;
    GraphSerializer& operator=(const GraphSerializer&) = delete;
    GraphSerializer& operator=(GraphSerializer&&) = delete;

    DumpResult<void> load() const;
    DumpResult<void> dump() const;

    DumpResult<void> dumpCommit(const Commit& commit) const;

private:
    mutable std::mutex _mutex;

    Graph* _graph;
};

}
