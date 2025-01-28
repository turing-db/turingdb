#pragma once

#include "EntityID.h"

namespace db {

class LabelSetRange {
public:
    bool entityInRange(EntityID entityID) const {
        return (entityID - _firstID) < _count;
    }

    size_t getOffset(EntityID entityID) const {
        return (entityID - _firstID + _firstPos).getValue();
    }

    EntityID _firstID;
    size_t _firstPos = 0;
    size_t _count = 0;
};

}
