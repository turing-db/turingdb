#pragma once

#include <range/v3/view/zip.hpp>

#include "metadata/GraphMetadata.h"
#include "LabelMapComparator.h"
#include "LabelSetMapComparator.h"
#include "PropertyTypeMapComparator.h"
#include "EdgeTypeMapComparator.h"

namespace db {

class GraphMetadataComparator {
public:
    [[nodiscard]] static bool same(const GraphMetadata& a, const GraphMetadata& b) {
        if (!LabelMapComparator::same(a.labels(), b.labels())) {
            return false;
        }

        if (!LabelSetMapComparator::same(a.labelsets(), b.labelsets())) {
            return false;
        }

        if (!EdgeTypeMapComparator::same(a.edgeTypes(), b.edgeTypes())) {
            return false;
        }

        if (!PropertyTypeMapComparator::same(a.propTypes(), b.propTypes())) {
            return false;
        }

        return true;
    }
};

}
