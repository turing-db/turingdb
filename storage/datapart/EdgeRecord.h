#pragma once

#include "ID.h"

namespace db {

struct EdgeRecord {
    EdgeID _edgeID;
    NodeID _nodeID;
    NodeID _otherID;
    EdgeTypeID _edgeTypeID;
};

}
