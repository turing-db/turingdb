#pragma once

#include "ArcManager.h"
#include "DataPart.h"

namespace db {

class DataPartModifier {
    WeakArc<DataPart> deleteNode(const NodeID idToDelete, const WeakArc<DataPart> dp);
};

}
