#include "MetadataRebaser.h"

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <spdlog/fmt/fmt.h>

#include "Profiler.h"
#include "metadata/GraphMetadata.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/VersionControlException.h"
#include "writers/MetadataBuilder.h"

#include "BioAssert.h"

using namespace db;

bool MetadataRebaser::rebase(const GraphMetadata& theirs,
                             MetadataBuilder& ours,
                             CommitWriteBuffer& cwb) {
    Profile profile("MetadataRebaser::rebase");

    const auto& theirLabels = theirs.labels();
    const auto& theirLabelSets = theirs.labelsets();
    const auto& theirEdgeTypes = theirs.edgeTypes();
    const auto& theirPropTypes = theirs.propTypes();

    const auto& ourLabels = ours._metadata->labels();
    const auto& ourLabelsets = ours._metadata->labelsets();
    const auto& ourEdgeTypes = ours._metadata->edgeTypes();
    const auto& ourPropTypes = ours._metadata->propTypes();

    LabelMap newLabels = theirLabels;
    LabelSetMap newLabelsets = theirLabelSets;
    EdgeTypeMap newEdgeTypes = theirEdgeTypes;
    PropertyTypeMap newPropTypes = theirPropTypes;

    // Initialize mapping
    for (const auto& [id, value] : newLabels) {
        _labelMapping[id] = id;
    }

    for (const auto& [id, value] : newLabelsets) {
        _labelsetMapping[id] = newLabelsets.getValue(id).value();
    }

    for (const auto& [id, value] : newEdgeTypes) {
        _edgeTypeMapping[id] = id;
    }

    // Labels
    for (const auto& [ourID, name] : ourLabels) {
        const auto newID = newLabels.getOrCreate(*name);
        _labelMapping[ourID] = newID;

        if (newID != ourID) {
            _labelsChanged = true;
        }
    }

    _labelsetsChanged = _labelsChanged;

    // Edge types
    for (const auto& [ourID, name] : ourEdgeTypes) {
        const auto newID = newEdgeTypes.getOrCreate(*name);
        _edgeTypeMapping[ourID] = newID;

        if (newID != ourID) {
            _edgeTypesChanged = true;
        }
    }

    mapPropertyTypes(ourPropTypes, newPropTypes);

    // Labelsets
    LabelSet newLabelset;
    for (const auto& [prevID, prevValue] : ourLabelsets) {
        std::vector<LabelID> labels;
        prevValue->decompose(labels);

        // Build labelset with patched IDs
        newLabelset = LabelSet {};

        for (LabelID a = 0; a < _labelMapping.size(); a++) {
            const LabelID b = _labelMapping[a];
            if (prevValue->hasLabel(a)) {
                newLabelset.set(b);
            }
        }

        const auto newHandle = newLabelsets.getOrCreate(newLabelset);
        _labelsetMapping[prevID] = newHandle;

        if (newHandle.getID() != prevID) {
            _labelsetsChanged = true;
        }
    }

    // Rebase WriteBuffer node metadata
    auto& pendingNodes = cwb._pendingNodes;
    for (auto&& node : pendingNodes) {
        // Get the LabelIDs that made up the node's LabelSetHandle
        std::vector<LabelID> nodeLabelSetLabels;
        node.labelsetHandle.decompose(nodeLabelSetLabels);

        newLabelset = LabelSet {};

        // Check each LabelID, map it over to the new ID if it exists
        for (const LabelID id : nodeLabelSetLabels) {
            if (_labelMapping.contains(id)) {
                newLabelset.set(_labelMapping.at(id));
            }
        }
        node.labelsetHandle = newLabelsets.getOrCreate(newLabelset);

        for (auto& [id, value] : node.properties) {
             id = _propTypeMapping[id]._id;
        }
    }

    // Rebase WriteBuffer edge metadata
    auto& pendingEdges = cwb._pendingEdges;
    for (auto&& edge : pendingEdges) {
        edge.edgeType = _edgeTypeMapping[edge.edgeType]; // EdgeTypes remapped
        for (auto& [id, value] : edge.properties) {
            id = _propTypeMapping[id]._id; // PropertyTypeIDs remapped
        }
    }

    { // Rebase WriteBuffer node updates
        auto& updatedNodes = cwb._updatedNodes;
        for (auto& node : updatedNodes) {
            CommitWriteBuffer::UntypedProperty& prop = node._updatedValue;

            auto& [id, _] = prop;
            id = _propTypeMapping[id]._id;
        }
    }

    { // Rebase WriteBuffer node updates
        auto& updatedEdges = cwb._updatedEdges;
        for (auto& edge : updatedEdges) {
            CommitWriteBuffer::UntypedProperty& prop = edge._updatedValue;

            auto& [id, _] = prop;
            id = _propTypeMapping[id]._id;
        }
    }

    ours._metadata->_labelMap = std::move(newLabels);
    ours._metadata->_labelsetMap = std::move(newLabelsets);
    ours._metadata->_edgeTypeMap = std::move(newEdgeTypes);
    ours._metadata->_propTypeMap = std::move(newPropTypes);

    return true;
}

void MetadataRebaser::mapPropertyTypes(const PropertyTypeMap& ours, PropertyTypeMap& rebased) {
    for (const auto& [pt, value, embeddingDimension] : rebased) {
        _propTypeMapping[pt._id] = pt;
    }

    std::unordered_map<PropertyTypeID, std::string> renames;

    for (const auto& [ourPT, name, embeddingDimension] : ours) {
        const ValueType valueType = ourPT._valueType;
        const std::optional<PropertyType> held = rebased.get(*name);

        const bool heldWithAnotherType = held.has_value() && held->_valueType != valueType;
        if (!heldWithAnotherType) {
            continue;
        }

        const std::string_view valueTypeName = ValueTypeName::value(valueType);
        std::string& renamed = renames[ourPT._id];
        renamed = fmt::format("{} ({})", *name, valueTypeName);
        const std::optional<PropertyType> heldRenamed = rebased.get(renamed);

        const bool renamedHeldWithAnotherType = heldRenamed.has_value() && heldRenamed->_valueType != valueType;
        if (renamedHeldWithAnotherType) {
            throw VersionControlException(fmt::format("Property '{}' ({}) can not be filed under '{}': main holds '{}' as {}",
                                                      *name,
                                                      valueTypeName,
                                                      renamed,
                                                      renamed,
                                                      ValueTypeName::value(heldRenamed->_valueType)));
        }
    }

    std::unordered_map<PropertyTypeID, const PropertyTypeMap::Pair*> filedFrom;

    for (const PropertyTypeMap::Pair& our : ours) {
        const PropertyType& ourPT = our._pt;
        const auto rename = renames.find(ourPT._id);
        const std::string& rebasedName = rename != renames.end() ? rename->second : *our._name;
        const PropertyType newPT = rebased.getOrCreate(rebasedName, ourPT._valueType);

        const auto [filed, inserted] = filedFrom.emplace(newPT._id, &our);
        if (!inserted) {
            const PropertyTypeMap::Pair& other = *filed->second;
            throw VersionControlException(fmt::format("Properties '{}' ({}) and '{}' ({}) of the change can not both be filed under '{}'",
                                                      *other._name,
                                                      ValueTypeName::value(other._pt._valueType),
                                                      *our._name,
                                                      ValueTypeName::value(ourPT._valueType),
                                                      rebasedName));
        }

        bioassert(newPT._valueType == ourPT._valueType, "The rebase files a property under one of another value type");

        _propTypeMapping[ourPT._id] = newPT;

        const std::optional<size_t>& embeddingDimension = our._embeddingDimension;
        if (embeddingDimension) {
            rebased.getOrSetEmbeddingDimension(newPT._id, *embeddingDimension);
        }

        if (newPT._id != ourPT._id) {
            _propTypesChanged = true;
        }
    }
}
