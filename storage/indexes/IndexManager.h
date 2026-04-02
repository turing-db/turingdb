#pragma once

#include <memory>

#include "ArcManager.h"
#include "Index.h"

#include "ID.h"

#include "metadata/SupportedType.h"

namespace db {

class IndexManager {
public:
    IndexManager();
    ~IndexManager();

    template <SupportedType P>
    [[nodiscard]] WeakArc<Index> createNodeIndex(std::string_view indexName, PropertyTypeID ptID);

    template <SupportedType P>
    [[nodiscard]] WeakArc<Index> createEdgeIndex(std::string_view indexName, PropertyTypeID ptID);

private:
    std::vector<WeakArc<Index>> _nodeIndexes;
    std::vector<WeakArc<Index>>_edgeIndexes;

    std::unique_ptr<ArcManager<Index>> _indexes;
};

}
