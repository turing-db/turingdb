#pragma once

namespace db {

class TuringDB;

class SimpleGraph {
public:
    static void createSimpleGraph(TuringDB& db);

    SimpleGraph() = delete;
};

}
