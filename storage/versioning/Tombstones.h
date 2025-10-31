#pragma once

#include <ranges>

#include "ID.h"
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

    template <TypedInternalID IDT>
    bool contains(IDT id) const;

    size_t numNodes() const { return _nodeTombstones.size(); }
    size_t numEdges() const { return _edgeTombstones.size(); }

    bool hasNodes() const { return !_nodeTombstones.empty(); }
    bool hasEdges() const { return !_edgeTombstones.empty(); }

    const NodeTombstones& nodeTombstones() const { return _nodeTombstones; }
    const EdgeTombstones& edgeTombstones() const { return _edgeTombstones; }

private:
    friend class CommitWriteBuffer;
    friend class ChangeRebaser;

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

template <TypedInternalID IDT>
bool Tombstones::contains(IDT id) const {
    if constexpr (std::is_same_v<IDT, NodeID>) {
        return containsNode(id);
    }
    else {
        return containsEdge(id);
    }
}

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
