#pragma once

#include <memory>
#include <unordered_map>

#include "ArcManager.h"
#include "ID.h"
#include "Index.h"

namespace db {

class IndexManager {
public:
    using PropertyIndexMap = std::unordered_map<PropertyTypeID, Index*>;

    // TODO: Ensure label set handles are rebased
    using NodeIndexMap = std::unordered_map<LabelSetHandle, PropertyIndexMap>;
    using EdgeIndexMap = std::unordered_map<EdgeTypeID, PropertyIndexMap>;
private:
    PropertyIndexMap _nodeIndexes;
    PropertyIndexMap _edgeIndexes;

    std::unique_ptr<ArcManager<Index>> _indexes;
};

}
