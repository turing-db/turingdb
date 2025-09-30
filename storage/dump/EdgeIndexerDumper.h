#pragma once

#include "EdgeContainer.h"
#include "Profiler.h"
#include "indexers/EdgeIndexer.h"
#include "GraphDumpHelper.h"
#include "EdgeIndexerDumpConstants.h"

namespace db {

class EdgeIndexerDumper {
public:
    using Constants = EdgeIndexerDumperConstants;

    explicit EdgeIndexerDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const EdgeIndexer& indexer) {
        Profile profile {"EdgeIndexerDumper::dump"};

        const auto& outLabelsetIndexer = indexer.getOutsByLabelSet();
        const auto& inLabelsetIndexer = indexer.getInsByLabelSet();

        const std::span outs = indexer._edges->getOuts();
        const std::span ins = indexer._edges->getIns();

        const auto firstNodeID = indexer.getFirstNodeID();
        const auto firstEdgeID = indexer.getFirstEdgeID();
        const uint64_t coreNodeCount = indexer.getCoreNodeCount();
        const uint64_t patchNodeCount = indexer.getPatchNodeCount();
        const uint64_t nodeCount = patchNodeCount + coreNodeCount;

        // Node data page count
        const uint64_t nodePageCount = GraphDumpHelper::getPageCountForItems(
            nodeCount, Constants::NODE_DATA_COUNT_PER_PAGE);

        const size_t remainder = nodeCount % Constants::NODE_DATA_COUNT_PER_PAGE;
        const NodeEdgeData* nodeData = indexer.getNodeData().data();

        size_t offset = 0;
        for (size_t i = 0; i < nodePageCount; i++) {
            // New page
            _writer.nextPage();
            const bool isLastPage = (i == nodePageCount - 1);
            const size_t countInPage = isLastPage
                                         ? (remainder == 0 ? Constants::NODE_DATA_COUNT_PER_PAGE : remainder)
                                         : Constants::NODE_DATA_COUNT_PER_PAGE;

            const std::span dataSpan = {nodeData + offset, countInPage};
            offset += countInPage;

            // Header
            _writer.writeToCurrentPage(countInPage);

            // Data
            for (const auto& data : dataSpan) {
                _writer.writeToCurrentPage(data._outRange._first);
                _writer.writeToCurrentPage(data._outRange._count);
                _writer.writeToCurrentPage(data._inRange._first);
                _writer.writeToCurrentPage(data._inRange._count);
            }
        }

        auto* buffer = &_writer.buffer();
        uint64_t outSpansPageCount = 1;
        uint64_t inSpansPageCount = 1;

        // Out edges labelset indexer
        if (outLabelsetIndexer.size() == 0) {
            outSpansPageCount = 0;
        } else {
            uint64_t indexerCountInPage = 0;

            _writer.nextPage();
            _writer.reserveSpace(Constants::LABELSET_INDEXER_PAGE_HEADER_STRIDE);

            for (const auto& [labelset, spans] : outLabelsetIndexer) {
                const uint64_t count = spans.size();
                const size_t stride = Constants::BASE_LABELSET_INDEXER_STRIDE
                                    + count * Constants::EDGE_SPAN_STRIDE;

                // A labelset indexer must fit into a single page
                if (count > Constants::MAX_LABELSET_INDEXER_SIZE) {
                    return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_INDEXER);
                }

                if (buffer->avail() < stride) {
                    // Fill page header
                    buffer->patch(reinterpret_cast<const uint8_t*>(&indexerCountInPage), sizeof(uint64_t), 0);

                    // Next page
                    outSpansPageCount++;
                    _writer.nextPage();
                    _writer.reserveSpace(Constants::LABELSET_INDEXER_PAGE_HEADER_STRIDE);
                    buffer = &_writer.buffer();
                    indexerCountInPage = 0;
                }

                _writer.writeToCurrentPage(count); // Number of spans in indexer
                _writer.writeToCurrentPage(labelset.getID().getValue());

                for (const auto& span : spans) {
                    const uint64_t count = span.size();
                    const size_t offset = std::distance(outs.data(), span.data());
                    _writer.writeToCurrentPage(offset);
                    _writer.writeToCurrentPage(count);
                }

                indexerCountInPage++;
            }

            buffer->patch(reinterpret_cast<const uint8_t*>(&indexerCountInPage), sizeof(uint64_t), 0);
        }

        // in edges labelset indexer
        if (inLabelsetIndexer.size() == 0) {
            inSpansPageCount = 0;
        } else {
            uint64_t indexerCountInPage = 0;

            _writer.nextPage();
            _writer.reserveSpace(Constants::LABELSET_INDEXER_PAGE_HEADER_STRIDE);

            for (const auto& [labelset, spans] : inLabelsetIndexer) {
                const uint64_t count = spans.size();
                const size_t stride = Constants::BASE_LABELSET_INDEXER_STRIDE
                                    + count * Constants::EDGE_SPAN_STRIDE;

                // A labelset indexer must fit into a single page
                if (count > Constants::MAX_LABELSET_INDEXER_SIZE) {
                    return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_INDEXER);
                }

                if (buffer->avail() < stride) {
                    // Fill page header
                    buffer->patch(reinterpret_cast<const uint8_t*>(&indexerCountInPage), sizeof(uint64_t), 0);

                    // Next page
                    inSpansPageCount++;
                    _writer.nextPage();
                    _writer.reserveSpace(Constants::LABELSET_INDEXER_PAGE_HEADER_STRIDE);
                    buffer = &_writer.buffer();
                    indexerCountInPage = 0;
                }

                _writer.writeToCurrentPage(count); // Number of spans in indexer
                _writer.writeToCurrentPage(labelset.getID().getValue());

                for (const auto& span : spans) {
                    const uint64_t count = span.size();
                    const size_t offset = std::distance(ins.data(), span.data());
                    _writer.writeToCurrentPage(offset);
                    _writer.writeToCurrentPage(count);
                }

                indexerCountInPage++;
            }

            buffer->patch(reinterpret_cast<const uint8_t*>(&indexerCountInPage), sizeof(uint64_t), 0);
        }

        // Back to beginning to write metadata
        _writer.seek(0);
        GraphDumpHelper::writeFileHeader(_writer);
        _writer.writeToCurrentPage(firstNodeID.getValue());
        _writer.writeToCurrentPage(firstEdgeID.getValue());
        _writer.writeToCurrentPage(coreNodeCount);
        _writer.writeToCurrentPage(patchNodeCount);
        _writer.writeToCurrentPage(nodePageCount);
        _writer.writeToCurrentPage(outSpansPageCount);
        _writer.writeToCurrentPage(inSpansPageCount);

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_INDEXER,
                                     _writer.error().value());
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

}

