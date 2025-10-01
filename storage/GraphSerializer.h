#pragma once

#include "dump/DumpResult.h"

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

private:
    Graph* _graph {nullptr};
};

}
