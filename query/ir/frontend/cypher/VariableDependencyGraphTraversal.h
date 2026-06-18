#pragma once

#include <stdint.h>

#include <vector>

namespace db {

class VariableDependency;
class VariableDependencyGraph;
class DependencyEdge;

class VariableDependencyGraphTraversal {
public:
    enum class Generator : uint8_t {
        SCAN_NODES = 0,
        GET_OUT_EDGES,
        GET_IN_EDGES,
        GET_EDGES,
        MERGE,

        _SIZE
    };

    struct Visit {
        const VariableDependency* _var {nullptr};
        const VariableDependency* _fstProducer {nullptr};
        const VariableDependency* _sndProducer {nullptr};
        Generator _gen {Generator::_SIZE};
    };

    void computeTraversal(const VariableDependencyGraph* graph,
                          std::vector<Visit>& traversal);
};

}
