#pragma once

#include <unordered_map>

#include "ID.h"
#include "LabelSetIndexer.h"

namespace db {

struct PropertyRange {
    size_t _offset {0};
    size_t _count {0};
};

using LabelSetPropertyIndexer = LabelSetIndexer<std::vector<PropertyRange>>;

using PropertyIndexer = std::unordered_map<PropertyTypeID, LabelSetPropertyIndexer>;

}
