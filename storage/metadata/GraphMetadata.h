#pragma once

#include "metadata/EdgeTypeMap.h"
#include "metadata/LabelMap.h"
#include "metadata/LabelSetMap.h"
#include "metadata/PropertyTypeMap.h"

namespace db {

class MetadataBuilder;
class MetadataRebaser;
class GraphLoader;

class GraphMetadata {
public:
    [[nodiscard]] const EdgeTypeMap& edgeTypes() const { return _edgeTypeMap; }
    [[nodiscard]] const LabelMap& labels() const { return _labelMap; }
    [[nodiscard]] const LabelSetMap& labelsets() const { return _labelsetMap; }
    [[nodiscard]] const PropertyTypeMap& propTypes() const { return _propTypeMap; }

private:
    friend MetadataBuilder;
    friend MetadataRebaser;
    friend GraphLoader;

    EdgeTypeMap _edgeTypeMap;
    LabelMap _labelMap;
    LabelSetMap _labelsetMap;
    PropertyTypeMap _propTypeMap;
};

}
