#pragma once

#include "types/EdgeTypeMap.h"
#include "labels/LabelMap.h"
#include "labels/LabelSetMap.h"
#include "types/PropertyTypeMap.h"

namespace db {

class MetadataBuilder;
class MetadataRebaser;
class GraphMetadataLoader;

class GraphMetadata {
public:
    [[nodiscard]] const EdgeTypeMap& edgeTypes() const { return _edgeTypeMap; }
    [[nodiscard]] const LabelMap& labels() const { return _labelMap; }
    [[nodiscard]] const LabelSetMap& labelsets() const { return _labelsetMap; }
    [[nodiscard]] const PropertyTypeMap& propTypes() const { return _propTypeMap; }

private:
    friend MetadataBuilder;
    friend MetadataRebaser;
    friend GraphMetadataLoader;

    EdgeTypeMap _edgeTypeMap;
    LabelMap _labelMap;
    LabelSetMap _labelsetMap;
    PropertyTypeMap _propTypeMap;
};

}
