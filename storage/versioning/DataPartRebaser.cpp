#include "DataPartRebaser.h"

#include "MetadataRebaser.h"
#include "EntityIDRebaser.h"
#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "datapart/NodeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "properties/PropertyContainer.h"
#include "properties/PropertyManager.h"
#include "indexers/PropertyIndexer.h"

#include "Profiler.h"
#include "BioAssert.h"

using namespace db;

bool DataPartRebaser::rebase(const MetadataRebaser& metadata,
                             const DataPart& prevPart,
                             DataPart& part) {
    Profile profile("DataPartRebaser::rebase");

    bioassert(_idRebaser, "invalid _idRebaser");

    auto& nodes = part._nodes;
    auto& edges = part._edges;
    auto& edgeIndexer = part._edgeIndexer;
    auto& nodeProperties = part._nodeProperties;
    auto& edgeProperties = part._edgeProperties;

    _prevFirstNodeID = part._firstNodeID;
    _prevFirstEdgeID = part._firstEdgeID;

    const auto newFirstNodeID = prevPart._firstNodeID + prevPart.getNodeContainerSize();
    const auto newFirstEdgeID = prevPart._firstEdgeID + prevPart.getEdgeContainerSize();
    _nodeOffset = (newFirstNodeID - _prevFirstNodeID).getValue();
    _edgeOffset = (newFirstEdgeID - _prevFirstEdgeID).getValue();

    part._firstNodeID = newFirstNodeID;
    part._firstEdgeID = newFirstEdgeID;
    part._nodes->_firstID = newFirstNodeID;
    part._edges->_firstNodeID = newFirstNodeID;
    part._edges->_firstEdgeID = newFirstEdgeID;
    part._edgeIndexer->_firstNodeID = newFirstNodeID;
    part._edgeIndexer->_firstEdgeID = newFirstEdgeID;

    { // @ref MetadataRebaser::rebase updates the Label(Set)Maps, so get new mapping
        for (auto& n : nodes->_nodes) {
            n._labelset = metadata.getLabelSetMapping(n._labelset.getID());
        }

        {
            LabelSetIndexer<NodeRange> newRanges;
            for (const auto& [labelset, range] : nodes->_ranges) {
                const auto newLabelSet = metadata.getLabelSetMapping(labelset.getID());
                auto& r = newRanges[newLabelSet];
                r = range;
                r._first = _idRebaser->rebaseNodeID(r._first);
            }
            nodes->_ranges = std::move(newRanges);
        }
    }


    { // @ref MetadataRebaser::rebase updates EdgeTypeMap, so get new mapping
        for (auto& e : edges->_outEdges) {
            e._edgeTypeID = metadata.getEdgeTypeMapping(e._edgeTypeID);
        }

        for (auto& e : edges->_inEdges) {
            e._edgeTypeID = metadata.getEdgeTypeMapping(e._edgeTypeID);
        }
    }

    { // Rebase all source, target, and edge IDs
        for (auto& e : edges->_outEdges) {
            e._nodeID = _idRebaser->rebaseNodeID(e._nodeID);
            e._otherID = _idRebaser->rebaseNodeID(e._otherID);
            e._edgeID = _idRebaser->rebaseEdgeID(e._edgeID);
        }

        for (auto& e : edges->_inEdges) {
            e._nodeID = _idRebaser->rebaseNodeID(e._nodeID);
            e._otherID = _idRebaser->rebaseNodeID(e._otherID);
            e._edgeID = _idRebaser->rebaseEdgeID(e._edgeID);
        }
    }

    { // Rebase patch node edges
        std::unordered_map<NodeID, size_t> newPatchOffsets;
        for (auto& [patchNode, index] : edgeIndexer->_patchNodeOffsets) {
            newPatchOffsets[_idRebaser->rebaseNodeID(patchNode)] = index;
        }

        edgeIndexer->_patchNodeOffsets.swap(newPatchOffsets);
    }

    { // Update spans for edge indexers, according to new LabelSetMap
        using EdgeSpan = std::span<const EdgeRecord>;
        using EdgeSpans = std::vector<EdgeSpan>;

        LabelSetIndexer<EdgeSpans> newOutLabelSetSpans;
        LabelSetIndexer<EdgeSpans> newInLabelSetSpans;

        for (const auto& [labelset, spans] : edgeIndexer->_outLabelSetSpans) {
            const auto newLabelSet = metadata.getLabelSetMapping(labelset.getID());
            newOutLabelSetSpans[newLabelSet] = spans;
        }

        for (const auto& [labelset, spans] : edgeIndexer->_inLabelSetSpans) {
            const auto newLabelSet = metadata.getLabelSetMapping(labelset.getID());
            newInLabelSetSpans[newLabelSet] = spans;
        }

        edgeIndexer->_outLabelSetSpans = std::move(newOutLabelSetSpans);
        edgeIndexer->_inLabelSetSpans = std::move(newInLabelSetSpans);
    }

    // Node properties
    { // @ref MetadataRebaser::rebase updates PropTypeMap, so get new mapping
        std::unordered_map<PropertyTypeID, std::unique_ptr<PropertyContainer>> newContainers;
        std::unordered_map<PropertyTypeID, PropertyContainer*> uint64s;
        std::unordered_map<PropertyTypeID, PropertyContainer*> int64s;
        std::unordered_map<PropertyTypeID, PropertyContainer*> doubles;
        std::unordered_map<PropertyTypeID, PropertyContainer*> strings;
        std::unordered_map<PropertyTypeID, PropertyContainer*> bools;
        std::unordered_map<PropertyTypeID, PropertyContainer*> embeddings;

        for (auto& [ptID, container] : nodeProperties->_map) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            newContainers[newPT._id] =
                std::unique_ptr<PropertyContainer> {container.release()};
        }

        for (const auto& [ptID, container] : nodeProperties->_uint64s) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            uint64s[newPT._id] = container;
        }

        for (const auto& [ptID, container] : nodeProperties->_int64s) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            int64s[newPT._id] = container;
        }

        for (const auto& [ptID, container] : nodeProperties->_doubles) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            doubles[newPT._id] = container;
        }

        for (const auto& [ptID, container] : nodeProperties->_strings) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            strings[newPT._id] = container;
        }

        for (const auto& [ptID, container] : nodeProperties->_bools) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            bools[newPT._id] = container;
        }

        for (const auto& [ptID, container] : nodeProperties->_embeddings) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            embeddings[newPT._id] = container;
        }

        nodeProperties->_map = std::move(newContainers);
        nodeProperties->_uint64s = std::move(uint64s);
        nodeProperties->_int64s = std::move(int64s);
        nodeProperties->_doubles = std::move(doubles);
        nodeProperties->_strings = std::move(strings);
        nodeProperties->_bools = std::move(bools);
        nodeProperties->_embeddings = std::move(embeddings);
        static_assert((size_t)ValueType::_SIZE == 7 && "A value type was added");


        {
            PropertyIndexer newIndexers;

            for (const auto& [ptID, indexer] : nodeProperties->_indexers) {
                const auto newPtID = metadata.getPropertyTypeMapping(ptID)._id;

                for (const auto& [labelset, ranges] : indexer) {
                    const auto newLabelSet = metadata.getLabelSetMapping(labelset.getID());
                    newIndexers[newPtID][newLabelSet] = ranges;
                }
            }

            nodeProperties->_indexers = std::move(newIndexers);
        }

        for (auto& [ptID, container] : nodeProperties->_map) {
            for (auto& id : container->getMutableIDs()) {
                id = _idRebaser->rebaseNodeID(id.getValue()).getValue();
            }
            container->sort();
        }
    }

    // Edge properties
    {
        std::unordered_map<PropertyTypeID, std::unique_ptr<PropertyContainer>> newContainers;
        std::unordered_map<PropertyTypeID, PropertyContainer*> uint64s;
        std::unordered_map<PropertyTypeID, PropertyContainer*> int64s;
        std::unordered_map<PropertyTypeID, PropertyContainer*> doubles;
        std::unordered_map<PropertyTypeID, PropertyContainer*> strings;
        std::unordered_map<PropertyTypeID, PropertyContainer*> bools;
        std::unordered_map<PropertyTypeID, PropertyContainer*> embeddings;

        for (auto& [ptID, container] : edgeProperties->_map) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            newContainers[newPT._id] =
                std::unique_ptr<PropertyContainer> {container.release()};
        }

        for (const auto& [ptID, container] : edgeProperties->_uint64s) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            uint64s[newPT._id] = container;
        }

        for (const auto& [ptID, container] : edgeProperties->_int64s) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            int64s[newPT._id] = container;
        }

        for (const auto& [ptID, container] : edgeProperties->_doubles) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            doubles[newPT._id] = container;
        }

        for (const auto& [ptID, container] : edgeProperties->_strings) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            strings[newPT._id] = container;
        }

        for (const auto& [ptID, container] : edgeProperties->_bools) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            bools[newPT._id] = container;
        }

        for (const auto& [ptID, container] : edgeProperties->_embeddings) {
            const auto newPT = metadata.getPropertyTypeMapping(ptID);
            embeddings[newPT._id] = container;
        }

        edgeProperties->_map = std::move(newContainers);
        edgeProperties->_uint64s = std::move(uint64s);
        edgeProperties->_int64s = std::move(int64s);
        edgeProperties->_doubles = std::move(doubles);
        edgeProperties->_strings = std::move(strings);
        edgeProperties->_bools = std::move(bools);
        edgeProperties->_embeddings = std::move(embeddings);
        static_assert((size_t)ValueType::_SIZE == 7 && "A value type was added");

        { // Update property indexers based on our new LabelSetMapping
            PropertyIndexer newIndexers;

            for (const auto& [ptID, indexer] : edgeProperties->_indexers) {
                const auto newPtID = metadata.getPropertyTypeMapping(ptID)._id;

                for (const auto& [labelset, ranges] : indexer) {
                    const auto newLabelSet = metadata.getLabelSetMapping(labelset.getID());
                    newIndexers[newPtID][newLabelSet] = ranges;
                }
            }

            edgeProperties->_indexers = std::move(newIndexers);
        }

        for (auto& [ptID, container] : edgeProperties->_map) {
            for (auto& id : container->getMutableIDs()) {
                id = _idRebaser->rebaseEdgeID(id.getValue()).getValue();
            }
            container->sort();
        }
    }

    return true;
}
