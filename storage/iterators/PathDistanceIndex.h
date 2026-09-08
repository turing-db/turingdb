#pragma once

#include <optional>
#include <span>
#include <stdint.h>
#include <stddef.h>
#include <vector>

#include "PathExplorationDir.h"
#include "datapart/EdgeRecord.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"
#include "ID.h"

namespace db {

class EdgeIndexer;
class PartDirectory;
class Tombstones;

// For every node, the fewest hops to a node carrying the end labels along the exploration
// direction: one breadth-first search from those nodes over the reverse direction. The
// distances ignore trail uniqueness and hop predicates, so each is a lower bound on the
// hops any trail still needs, and pruning by it never drops a valid path.
class PathDistanceIndex {
public:
    static constexpr uint8_t unreachable = 255;
    static constexpr uint8_t farthest = 254;

    PathDistanceIndex();
    ~PathDistanceIndex();

    void build(const GraphView& view,
               const LabelSet& endLabels,
               PathExplorationDir direction,
               std::optional<EdgeTypeID> edgeType,
               uint64_t maxHops);

    bool isBuilt() const { return _built; }
    uint8_t getDistance(NodeID node) const;
    bool isEnd(NodeID node) const { return getDistance(node) == 0; }
    bool canReachEndWithin(NodeID node, uint64_t hops) const;
    size_t getReachedCount() const { return _reached; }

    // What a walk of one edge type branches by at the nodes it reaches, and how many nodes
    // its frontier can occupy. A relation held by a fraction of the nodes is walked only
    // through those, so averaging its degree over every node describes no walk at all.
    // Sampled with a stride: no per-type edge count is kept.
    struct TypeBranching {
        double _fanOut {0.0};
        double _supportNodes {0.0};
    };

    static void sampleBranching(const PartDirectory& parts,
                                PathExplorationDir direction,
                                std::optional<EdgeTypeID> edgeType,
                                TypeBranching& branching);

    // The candidate checks the unpruned walk is expected to make: the seeds fanning out over
    // every hop of the bound, the frontier holding once it covers the nodes the type reaches.
    // Sampling the branching costs a strided pass over the adjacency, so a caller that needs
    // more than one of these takes the sample once and passes it to the overload.
    static double estimatedEnumerationChecks(const PartDirectory& parts,
                                             PathExplorationDir direction,
                                             std::optional<EdgeTypeID> edgeType,
                                             size_t seedCount,
                                             uint64_t maxHops);

    static double estimatedEnumerationChecks(const PartDirectory& parts,
                                             const TypeBranching& branching,
                                             size_t seedCount,
                                             uint64_t maxHops);

    // Whether the enumeration the seeds imply is expected to cost more than the index
    static bool isWorthBuilding(const GraphView& view,
                                PathExplorationDir direction,
                                std::optional<EdgeTypeID> edgeType,
                                size_t seedCount,
                                uint64_t maxHops);

private:
    std::vector<uint8_t> _distances;
    size_t _reached {0};
    bool _built {false};

    void collectEnds(const PartDirectory& parts, const LabelSet& endLabels, std::vector<NodeID>& ends);
    void relax(std::span<const EdgeRecord> edges,
               uint8_t level,
               std::optional<EdgeTypeID> edgeType,
               const Tombstones* tombstones,
               std::vector<NodeID>& next);
};

}
