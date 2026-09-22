#pragma once

#include <array>
#include <optional>
#include <span>
#include <stdint.h>
#include <stddef.h>
#include <vector>

#include "PathExplorationDir.h"
#include "datapart/EdgeRecord.h"
#include "metadata/EdgeBranchingCache.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"
#include "ID.h"

namespace db {

class EdgeIndexer;
class PartDirectory;
class PathHopFilter;
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
               std::span<const EdgeTypeID> edgeTypes,
               uint64_t maxHops);

    // The same search from a list of ends, the hops to the nearest of them
    void build(const GraphView& view,
               std::span<const NodeID> ends,
               PathExplorationDir direction,
               std::span<const EdgeTypeID> edgeTypes,
               uint64_t maxHops);

    bool isBuilt() const { return _built; }

    // Forgets the search, so an index reused for another shape does not answer from it
    void clear();

    uint8_t getDistance(NodeID node) const;
    bool isEnd(NodeID node) const { return getDistance(node) == 0; }
    bool canReachEndWithin(NodeID node, uint64_t hops) const;
    size_t getReachedCount() const { return _reached; }

    // Sampled with a stride, since no per-type edge count is kept, and memoised on the
    // parts it was measured on
    using TypeBranching = EdgeBranching;

    static void sampleBranching(const PartDirectory& parts,
                                PathExplorationDir direction,
                                std::span<const EdgeTypeID> edgeTypes,
                                TypeBranching& branching);

    // The share of a strided sample's candidates the hop predicate keeps. The branching
    // sampled off the edge type is what the walk would do with no predicate on its hops;
    // scaling it by this is what the walk actually does, and a selective predicate is the
    // difference between an enumeration that dwarfs an index and one that costs nothing.
    // Costs a pass over the sample's adjacency and an evaluation of the predicate on it,
    // so the gates take it only once the cheap estimate has said the index is worth it.
    static double sampleHopPassRate(const PartDirectory& parts,
                                    PathExplorationDir direction,
                                    std::span<const EdgeTypeID> edgeTypes,
                                    PathHopFilter& hopFilter);

    // What a sample of a walk's own seeds expanded to, level by level, and the ratio the last
    // measured level grew by. A walk's frontier is not one fan-out held constant: seeds that
    // die at once hold the early levels down while the region the survivors reach decides
    // every later one, so no single number stands for both and the levels that were measured
    // are the estimate rather than a parameter of it.
    struct SeedExpansion {
        static constexpr size_t maxLevels = 8;

        std::array<double, maxLevels> _frontierPerSeed {};
        size_t _levels {0};
        double _tailFanOut {1.0};
    };

    // Expands a sample of the seeds along the walk's direction, one level at a time. What a
    // walk costs turns on the fan-out of where it starts, and an average over every node
    // carrying the type misses that by the exponent of the bound.
    static void sampleSeedExpansion(const PartDirectory& parts,
                                    PathExplorationDir direction,
                                    std::span<const EdgeTypeID> edgeTypes,
                                    std::span<const NodeID> seeds,
                                    SeedExpansion& expansion);

    // The candidate checks one search from each source is expected to make: its frontier is
    // distinct nodes, so it cannot grow past the nodes the type reaches and holds at the
    // first level that covers them.
    static double estimatedSearchChecks(const PartDirectory& parts,
                                        PathExplorationDir direction,
                                        std::span<const EdgeTypeID> edgeTypes,
                                        size_t sourceCount,
                                        uint64_t maxHops);

    // The candidate checks the unpruned walk is expected to make: the levels the sample
    // measured as measured, and past them the frontier growing by the last ratio it saw. Its
    // frontier is partial paths, not nodes - a trail reaches the same node as often as a path
    // arrives at it - so nothing caps it at the nodes the type carries.
    // hopPassRate is the share of each level's candidates the query's hop predicate lets
    // through: they all cost a check, and the ones that pass are all that reach the next
    // level, so it shrinks the frontier rather than the candidates.
    static double estimatedEnumerationChecks(const PartDirectory& parts,
                                             const SeedExpansion& expansion,
                                             size_t seedCount,
                                             uint64_t maxHops,
                                             double hopPassRate = 1.0);

    // The candidate checks a search from that many sources costs to build: the nodes and edges
    // it touches, at most the graph, and the distance byte filled for every node
    static double estimatedBuildChecks(const PartDirectory& parts,
                                       PathExplorationDir direction,
                                       std::span<const EdgeTypeID> edgeTypes,
                                       size_t sourceCount,
                                       uint64_t maxHops);

    // Whether the enumeration the seeds imply is expected to cost more than the index
    static bool isWorthBuilding(const GraphView& view,
                                const SeedExpansion& expansion,
                                size_t seedCount,
                                uint64_t maxHops,
                                double hopPassRate = 1.0);

private:
    std::vector<uint8_t> _distances;
    size_t _reached {0};
    bool _built {false};

    void collectEnds(const PartDirectory& parts, const LabelSet& endLabels, std::vector<NodeID>& ends);
    void search(const PartDirectory& parts,
                const Tombstones& tombstones,
                std::vector<NodeID>& frontier,
                PathExplorationDir direction,
                std::span<const EdgeTypeID> edgeTypes,
                uint64_t maxHops);
    void relax(std::span<const EdgeRecord> edges,
               uint8_t level,
               std::span<const EdgeTypeID> edgeTypes,
               const Tombstones* tombstones,
               std::vector<NodeID>& next);
};

}
