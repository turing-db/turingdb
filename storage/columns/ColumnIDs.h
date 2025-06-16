#pragma once

#include "ColumnVector.h"
#include "ColumnConst.h"
#include "ID.h"

namespace db {

using ColumnNodeIDs = ColumnVector<NodeID>;
using ColumnNodeID = ColumnConst<NodeID>;
using ColumnEdgeIDs = ColumnVector<EdgeID>;
using ColumnEdgeID = ColumnConst<EdgeID>;

}
