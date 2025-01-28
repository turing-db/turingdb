#pragma once

namespace db {

class Graph;

class GraphComparator {
public:
    [[nodiscard]] static bool same(const Graph& a, const Graph& b);
};

}
