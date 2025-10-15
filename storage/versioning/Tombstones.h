#pragma once

#include <ranges>

#include "versioning/TombstoneSet.h"

namespace db {

class Tombstones {
public:
    using NodeTombstones = TombstoneSet<NodeID>;
    using EdgeTombstones = TombstoneSet<EdgeID>;

    Tombstones() = default;
    ~Tombstones() = default;

    // Keep copy constructors for creating tombstones for next commit
    Tombstones(const Tombstones&) = default;
    Tombstones& operator=(const Tombstones&) = default;

    // Delete move constructors as previous commits need to keep their tombstones
    Tombstones(Tombstones&&) = delete;
    Tombstones& operator=(Tombstones&&) = delete;

    bool containsNode(NodeID nodeID) const { return _nodeTombstones.contains(nodeID); }
    bool containsEdge(EdgeID edgeID) const { return _edgeTombstones.contains(edgeID); }

    size_t numNodes() const { return _nodeTombstones.size(); }
    size_t numEdges() const { return _edgeTombstones.size(); }

    bool hasNodes() const { return !_nodeTombstones.empty(); }
    bool hasEdges() const { return !_edgeTombstones.empty(); }

private:
    friend class CommitWriteBuffer;

    /**
     * @brief Given a range over NodeIDs, calls @ref TombstoneSet::insert over that range
     */
    template <std::ranges::input_range Range>
        requires std::same_as<std::ranges::range_value_t<Range>, NodeID>
    void addNodeTombstones(Range& nodes);

    /**
     * @brief Given a range over EdgeIDs, calls @ref TombstoneSet::insert over that range
     */
    template <std::ranges::input_range Range>
        requires std::same_as<std::ranges::range_value_t<Range>, EdgeID>
    void addEdgeTombstones(Range& edges);

    NodeTombstones _nodeTombstones;
    EdgeTombstones _edgeTombstones;
};

template <std::ranges::input_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, NodeID>
void Tombstones::addNodeTombstones(Range& nodes) {
    _nodeTombstones.insert(nodes);
}

template <std::ranges::input_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, EdgeID>
void Tombstones::addEdgeTombstones(Range& edges) {
    _edgeTombstones.insert(edges);
}
}
