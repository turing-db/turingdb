#pragma once

#include "labels/LabelMap.h"
#include "labels/LabelSetMap.h"
#include "types/EdgeTypeMap.h"
#include "types/PropertyTypeMap.h"

namespace db {

class GraphMetadata {
public:
    const LabelMap& labels() const { return _labelMap; };
    const LabelSetMap& labelsets() const { return _labelsetMap; }

    const EdgeTypeMap& edgeTypes() const { return _edgeTypeMap; }
    const PropertyTypeMap& propTypes() const { return _propTypeMap; }

    LabelMap& labels() { return _labelMap; };
    LabelSetMap& labelsets() { return _labelsetMap; }

    EdgeTypeMap& edgeTypes() { return _edgeTypeMap; }
    PropertyTypeMap& propTypes() { return _propTypeMap; }

private:
    LabelMap _labelMap;
    LabelSetMap _labelsetMap;
    PropertyTypeMap _propTypeMap;
    EdgeTypeMap _edgeTypeMap;
};

}
