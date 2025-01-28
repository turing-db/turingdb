#pragma once

#include "EntityID.h"
#include "LabelSet.h"
#include "MetadataMap.h"

namespace db {

using LabelSetMap = MetadataMap<LabelSet, LabelSetID, LabelSetID::_max>;

}
