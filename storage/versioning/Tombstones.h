#pragma once

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

    // XXX: Is it dangerous to have this overloaded on both Node and Edge IDs?
    bool contains(NodeID nodeID) { return _nodeTombstones.contains(nodeID); }
    bool contains(EdgeID edgeID) { return _edgeTombstones.contains(edgeID); }

private:
    NodeTombstones _nodeTombstones;
    EdgeTombstones _edgeTombstones;
};

}
