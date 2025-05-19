#pragma once

#include "EntityID.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"

namespace db {

class GraphMetadata;
class MetadataBuilder;

class MetadataRebaser {
public:
    MetadataRebaser() = default;
    ~MetadataRebaser() = default;

    MetadataRebaser(const MetadataRebaser&) = delete;
    MetadataRebaser(MetadataRebaser&&) = delete;
    MetadataRebaser& operator=(const MetadataRebaser&) = delete;
    MetadataRebaser& operator=(MetadataRebaser&&) = delete;

    bool rebase(const GraphMetadata& theirs, MetadataBuilder& ours);

    LabelID getLabelMapping(LabelID id) const { return _labelMapping.at(id); }
    LabelSetHandle getLabelSetMapping(LabelSetID id) const { return _labelsetMapping.at(id); }
    EdgeTypeID getEdgeTypeMapping(EdgeTypeID id) const { return _edgeTypeMapping.at(id); }
    PropertyType getPropertyTypeMapping(PropertyTypeID id) const { return _propTypeMapping.at(id); }

    bool labelsChanged() const { return _labelsChanged; }
    bool labelsetsChanged() const { return _labelsetsChanged; }
    bool edgeTypesChanged() const { return _edgeTypesChanged; }
    bool propTypesChanged() const { return _propTypesChanged; }

    void clear() {
        _labelMapping.clear();
        _labelsetMapping.clear();
        _edgeTypeMapping.clear();
        _propTypeMapping.clear();

        _labelsChanged = false;
        _labelsetsChanged = false;
        _edgeTypesChanged = false;
        _propTypesChanged = false;
    }

private:
    std::unordered_map<LabelID, LabelID> _labelMapping;
    std::unordered_map<LabelSetID, LabelSetHandle> _labelsetMapping;
    std::unordered_map<EdgeTypeID, EdgeTypeID> _edgeTypeMapping;
    std::unordered_map<PropertyTypeID, PropertyType> _propTypeMapping;

    bool _labelsChanged {false};
    bool _labelsetsChanged {false};
    bool _edgeTypesChanged {false};
    bool _propTypesChanged {false};
};

}
