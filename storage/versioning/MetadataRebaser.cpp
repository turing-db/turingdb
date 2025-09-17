#include "MetadataRebaser.h"

#include "Profiler.h"
#include "metadata/GraphMetadata.h"
#include "writers/MetadataBuilder.h"

using namespace db;

bool MetadataRebaser::rebase(const GraphMetadata& theirs, MetadataBuilder& ours) {
    Profile profile {"MetadataRebaser::rebase"};

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

    for (const auto& [pt, value] : newPropTypes) {
        _propTypeMapping[pt._id] = pt;
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

    // Property types
    for (const auto& [ourPT, name] : ourPropTypes) {
        auto newPT = newPropTypes.getOrCreate(*name, ourPT._valueType);
        if (newPT._valueType != ourPT._valueType) {
            // Property type already exists, but with different value type
            // Creating a new property type with name = 'prevName (valueType)'
            const auto newName = fmt::format("{} ({})", *name, ValueTypeName::value(ourPT._valueType));
            newPT = newPropTypes.getOrCreate(newName, ourPT._valueType);
        }

        _propTypeMapping[ourPT._id] = newPT;

        if (newPT._id != ourPT._id) {
            _propTypesChanged = true;
        }
    }

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


    // TODO: Review setting of _labelsChanged, etc.
    // if (_labelsChanged) {
        ours._metadata->_labelMap = std::move(newLabels);
    // }

    // if (_labelsetsChanged) {
        ours._metadata->_labelsetMap = std::move(newLabelsets);
    // }

    // if (_edgeTypesChanged) {
        ours._metadata->_edgeTypeMap = std::move(newEdgeTypes);
    // }

    // if (_propTypesChanged) {
        ours._metadata->_propTypeMap = std::move(newPropTypes);
    // }

    return true;
}
