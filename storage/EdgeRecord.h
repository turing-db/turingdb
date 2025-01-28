#pragma once

#include "EntityID.h"

namespace db {

struct EdgeRecord {
    EntityID _edgeID;
    EntityID _nodeID;
    EntityID _otherID;
    EdgeTypeID _edgeTypeID;
};

}
