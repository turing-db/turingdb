#pragma once

#include <compare>
#include <limits>
#include <optional>
#include <span>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathHopFilter.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"
#include "ID.h"

namespace db {
class PathDistanceIndex;
}

namespace turing::test {

constexpr uint64_t unbounded = std::numeric_limits<uint64_t>::max();

struct ReferenceEdge {
    uint64_t _edge {0};
    uint64_t _other {0};
    uint64_t _type {0};
};

// The graph as the single-hop writers see it, the ground truth the explorator is compared to
struct Adjacency {
    std::vector<std::vector<ReferenceEdge>> _outs;
    std::vector<std::vector<ReferenceEdge>> _ins;
};

// One emitted row flattened to raw values: the input row, the end node and the path's edges
struct PathRow {
    size_t _index {0};
    uint64_t _target {0};
    std::vector<uint64_t> _edges;

    auto operator<=>(const PathRow&) const = default;
};

using HopPredicate = bool (*)(uint64_t source, uint64_t edge, uint64_t end);

void buildAdjacency(const db::GraphView& view, size_t nodeCount, Adjacency& adjacency);
uint64_t edgeBetween(const Adjacency& adjacency, uint64_t source, uint64_t target);

// A plain recursive trail enumerator over the adjacency, the oracle of every configuration
class ReferenceEnumerator {
public:
    ReferenceEnumerator(const Adjacency& adjacency,
                        db::PathExplorationDir direction,
                        uint64_t minHops,
                        uint64_t maxHops);

    void setEdgeType(uint64_t type) { _edgeType = type; }
    void setHopPredicate(HopPredicate predicate) { _predicate = predicate; }

    // Only the rows ending on a node the flag marks are kept, as an end constraint does
    void setEnds(const std::vector<bool>* ends) { _ends = ends; }

    void enumerate(const db::ColumnNodeIDs& seeds, std::vector<PathRow>& rows);

private:
    const Adjacency& _adjacency;
    db::PathExplorationDir _direction {db::PathExplorationDir::FORWARD};
    uint64_t _minHops {0};
    uint64_t _maxHops {0};
    std::optional<uint64_t> _edgeType;
    HopPredicate _predicate {nullptr};
    const std::vector<bool>* _ends {nullptr};

    void walk(size_t seedRow, uint64_t node, std::vector<uint64_t>& path, std::vector<PathRow>& rows);
    void descend(size_t seedRow,
                 uint64_t node,
                 const std::vector<ReferenceEdge>& candidates,
                 std::vector<uint64_t>& path,
                 std::vector<PathRow>& rows);
};

// Applies a hop predicate to a frame the way the query engine will: compacting the spans
class PredicateHopFilter : public db::PathHopFilter {
public:
    explicit PredicateHopFilter(HopPredicate predicate);
    ~PredicateHopFilter() override;

    size_t filter(db::NodeID source, std::span<db::NodeID> nodes, std::span<db::EdgeID> edges) override;

private:
    HopPredicate _predicate {nullptr};
};

struct ExplorationOptions {
    size_t _maxCount {db::ChunkConfig::CHUNK_SIZE};
    size_t _walkerCount {1};
    size_t _lookahead {1};
    std::optional<db::EdgeTypeID> _edgeType;
    db::PathHopFilter* _hopFilter {nullptr};
    const db::LabelSet* _endLabels {nullptr};
    const db::PathDistanceIndex* _distanceIndex {nullptr};
    bool _collectTargets {true};
    bool _collectPaths {true};
};

// Drives the explorator to exhaustion, expanding every emitted path through the trie, and
// returns how many edge records it examined. An edge appearing twice on one path fails
// here, whatever the configuration.
size_t collectPaths(const db::GraphView& view,
                    const db::ColumnNodeIDs& input,
                    db::PathExplorationDir direction,
                    uint64_t minHops,
                    uint64_t maxHops,
                    const ExplorationOptions& options,
                    std::vector<PathRow>& rows);

void expectSameRows(std::vector<PathRow> expected, std::vector<PathRow> actual);
size_t countRowsThrough(const std::vector<PathRow>& rows, uint64_t edge);

}
