#pragma once

#include <unordered_map>

#include "EntityID.h"
#include "LabelSetRange.h"
#include "LabelSetIndexer.h"

namespace db {

struct LabelSetInfo {
    std::vector<LabelSetRange> _ranges;
    bool _binarySearched = false;
};

using PropertyIndexer = std::unordered_map<PropertyTypeID, LabelSetIndexer<LabelSetInfo>>;

}
