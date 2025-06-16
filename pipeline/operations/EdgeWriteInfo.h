#pragma once

#include "columns/ColumnIDs.h"
#include "columns/ColumnEdgeTypes.h"

namespace db {

struct EdgeWriteInfo {
    ColumnVector<size_t>* _indices {nullptr};
    ColumnNodeIDs* _sourceNodes {nullptr};
    ColumnEdgeIDs* _edges {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _targetNodes {nullptr};
};

}
