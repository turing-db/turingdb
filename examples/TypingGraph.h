#pragma once

namespace db {

class Graph;

class TypingGraph {
public:
    static void createTypingGraph(Graph* graph);

    TypingGraph() = delete;
};

}
