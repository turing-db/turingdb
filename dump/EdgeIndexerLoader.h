#pragma once

#include <memory>

#include "EdgeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "FilePageReader.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "EdgeIndexerDumpConstants.h"
#include "metadata/GraphMetadata.h"

namespace db {

class EdgeIndexerLoader {
public:
    using Constants = EdgeIndexerDumperConstants;

    explicit EdgeIndexerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<EdgeIndexer>> load(const GraphMetadata& metadata, const EdgeContainer& edges) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER,
                                     _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        // Check if we received a full page
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER);
        }

        // Check file header
        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        const EntityID firstNodeID = it.get<EntityID::Type>();
        const EntityID firstEdgeID = it.get<EntityID::Type>();
        const uint64_t coreNodeCount = it.get<uint64_t>();
        const uint64_t patchNodeCount = it.get<uint64_t>();
        const uint64_t nodePageCount = it.get<uint64_t>();
        const uint64_t outSpansPageCount = it.get<uint64_t>();
        const uint64_t inSpansPageCount = it.get<uint64_t>();
        const uint64_t nodeCount = coreNodeCount + patchNodeCount;

        EdgeIndexer* indexer = new EdgeIndexer {edges};
        indexer->_firstNodeID = firstNodeID;
        indexer->_firstEdgeID = firstEdgeID;
        indexer->_nodes.resize(nodeCount);

        // Loading node edge data
        size_t offset = 0;
        for (size_t i = 0; i < nodePageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER,
                                         _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER);
            }

            const size_t countInPage = it.get<uint64_t>();

            // Copy content of page into nodes vector
            for (size_t j = 0; j < countInPage; j++) {
                auto& n = indexer->_nodes[j + offset];
                n._outRange._first = it.get<size_t>();
                n._outRange._count = it.get<size_t>();
                n._inRange._first = it.get<size_t>();
                n._inRange._count = it.get<size_t>();
            }

            offset += countInPage;
        }

        indexer->_patchNodes = {indexer->_nodes.data(), patchNodeCount};
        indexer->_coreNodes = {indexer->_nodes.data() + patchNodeCount, coreNodeCount};

        // Mapping patch node IDs to their positions in indexer->_nodes
        const auto& outs = edges.getOuts();
        const auto& ins = edges.getIns();
        for (size_t i = 0; i < patchNodeCount; i++) {
            const auto& data = indexer->_patchNodes[i];
            EntityID nodeID;

            if (data._outRange._count != 0) {
                nodeID = outs[data._outRange._first]._nodeID;
            }

            else if (data._inRange._count != 0) {
                nodeID = ins[data._inRange._first]._nodeID;
            }

            else {
                // Must have at least one edge if it was recorded as a patch
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER);
            }

            indexer->_patchNodeOffsets[nodeID] = i;
        }

        // Loading Out labelset indexers
        for (size_t i = 0; i < outSpansPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER,
                                         _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER);
            }

            const size_t indexerCountInPage = it.get<uint64_t>();

            for (size_t j = 0; j < indexerCountInPage; j++) {
                const uint64_t spanCount = it.get<uint64_t>();
                const LabelSetID labelsetID = it.get<LabelSetID::Type>();

                if (it.remainingBytes() < spanCount * Constants::EDGE_SPAN_STRIDE) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER);
                }

                const auto labelset = metadata.labelsets().getValue(labelsetID);

                if (!labelset) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER);
                }

                auto& spans = indexer->_outLabelSetSpans[labelset.value()];

                for (size_t k = 0; k < spanCount; k++) {
                    const uint64_t offset = it.get<uint64_t>();
                    const uint64_t count = it.get<uint64_t>();
                    spans.emplace_back(outs.data() + offset, count);
                }
            }
        }

        // Loading In labelset indexers
        for (size_t i = 0; i < inSpansPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER,
                                         _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER,
                                         _reader.error().value());
            }

            const size_t indexerCountInPage = it.get<uint64_t>();

            for (size_t j = 0; j < indexerCountInPage; j++) {
                const uint64_t spanCount = it.get<uint64_t>();
                const LabelSetID labelsetID = it.get<LabelSetID::Type>();

                if (it.remainingBytes() < spanCount * Constants::EDGE_SPAN_STRIDE) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER);
                }

                const auto labelset = metadata.labelsets().getValue(labelsetID);

                if (!labelset) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER);
                }

                auto& spans = indexer->_inLabelSetSpans[labelset.value()];

                for (size_t k = 0; k < spanCount; k++) {
                    const uint64_t offset = it.get<uint64_t>();
                    const uint64_t count = it.get<uint64_t>();
                    spans.emplace_back(ins.data() + offset, count);
                }
            }
        }

        return {std::unique_ptr<EdgeIndexer> {indexer}};
    }

private:
    fs::FilePageReader& _reader;
};

}
