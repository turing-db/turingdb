#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "Index.h"
#include "ArcManager.h"

namespace db {

class IndexManager {
public:
    using PropertyIndexMap = std::unordered_map<std::string, Index*>;
private:
    PropertyIndexMap _nodeIndexes;
    PropertyIndexMap _edgeIndexes;

    std::unique_ptr<ArcManager<Index>> _indexes;
};

}
