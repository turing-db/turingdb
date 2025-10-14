#pragma once

#include "versioning/TombstoneSet.h"

namespace db {

class Tombstones {
public:
    using NodeTombstones = TombstoneSet<NodeID>;
    using EdgeTombstones = TombstoneSet<EdgeID>;
private:
    NodeTombstones _nodeTombstones;
    EdgeTombstones _edgeTombstones;
};

}
