#pragma once

#include "GraphDumpHelper.h"
#include "NodeContainer.h"
#include "NodeContainerDumpConstants.h"

namespace db {

class NodeContainerDumper {
public:
    using Constants = NodeContainerDumperConstants;

    explicit NodeContainerDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const NodeContainer& nodes) {
        GraphDumpHelper::writeFileHeader(_writer);

        const auto& indexer = nodes.getLabelSetIndexer();
        const uint64_t rangeCount = indexer.size();
        const uint64_t nodeCount = nodes.size();

        // Page counts
        const uint64_t rangePageCount = GraphDumpHelper::getPageCountForItems(
            rangeCount, Constants::RANGE_COUNT_PER_PAGE);

        const uint64_t recordPageCount = GraphDumpHelper::getPageCountForItems(
            nodeCount, Constants::RECORD_COUNT_PER_PAGE);

        // Metadata
        _writer.writeToCurrentPage(nodes.getFirstNodeID().getValue());
        _writer.writeToCurrentPage(nodeCount);
        _writer.writeToCurrentPage(rangeCount);
        _writer.writeToCurrentPage(recordPageCount);
        _writer.writeToCurrentPage(rangePageCount);

        {
            // Ranges
            const size_t remainder = rangeCount % Constants::RANGE_COUNT_PER_PAGE;

            auto it = indexer.begin();
            for (size_t i = 0; i < rangePageCount; i++) {
                // New page
                _writer.nextPage();

                const bool isLastPage = (i == rangePageCount - 1);
                const size_t countInPage = isLastPage
                                             ? (remainder == 0 ? Constants::RANGE_COUNT_PER_PAGE : remainder)
                                             : Constants::RANGE_COUNT_PER_PAGE;

                // Header
                _writer.writeToCurrentPage(countInPage);

                // Data
                for (size_t j = 0; j < countInPage; j++) {
                    const auto& [lset, range] = *it;
                    _writer.writeToCurrentPage(lset.getID().getValue());
                    _writer.writeToCurrentPage(range._first.getValue());
                    _writer.writeToCurrentPage((uint64_t)range._count);
                    ++it;
                }
            }
        }

        {
            // Records
            const size_t remainder = nodeCount % Constants::RECORD_COUNT_PER_PAGE;
            const auto& records = nodes.records();

            size_t offset = 0;
            for (size_t i = 0; i < recordPageCount; i++) {
                // New page
                _writer.nextPage();

                const bool isLastPage = (i == recordPageCount - 1);
                const size_t countInPage = isLastPage
                                             ? (remainder == 0 ? Constants::RECORD_COUNT_PER_PAGE : remainder)
                                             : Constants::RECORD_COUNT_PER_PAGE;

                const std::span nodeSpan = std::span {records}.subspan(offset, countInPage);
                offset += countInPage;

                // Header
                _writer.writeToCurrentPage(countInPage);

                // Data
                for (const auto& record : nodeSpan) {
                    _writer.writeToCurrentPage(record._labelset.getID().getValue());
                }
            }
        }

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_NODES, _writer.error().value());
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

}

