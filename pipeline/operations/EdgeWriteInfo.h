#pragma once

#include "ColumnIDs.h"
#include "ColumnEdgeTypes.h"

namespace db {

struct EdgeWriteInfo {
    ColumnVector<size_t>* _indices {nullptr};
    ColumnIDs* _sourceNodes {nullptr};
    ColumnIDs* _edges {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnIDs* _targetNodes {nullptr};
};

}
