#pragma once

#include "ArcManager.h"
#include "DataPart.h"

namespace db {

class DataPartModifier {
    void deleteNode(const NodeID idToDelete, const WeakArc<DataPart> dp);
};

}
