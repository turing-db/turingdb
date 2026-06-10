#pragma once

#include "GraphDumpHelper.h"
#include "datapart/EdgeContainer.h"
#include "EdgeContainerDumpConstants.h"
#include "Profiler.h"

namespace db {

class EdgeContainerDumper {
public:
    using Constants = EdgeContainerDumperConstants;

    explicit EdgeContainerDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const EdgeContainer& edges) {
        Profile profile("EdgeContainerDumper::dump");
        GraphDumpHelper::writeFileHeader(_writer);

        const uint64_t edgeCount = edges.size();

        // Page count
        const uint64_t pageCountPerDir = GraphDumpHelper::getPageCountForItems(
            edgeCount, Constants::COUNT_PER_PAGE);

        // Metadata
        _writer.writeToCurrentPage((uint64_t)edges.getFirstEdgeID().getValue());
        _writer.writeToCurrentPage((uint64_t)edges.getFirstNodeID().getValue());
        _writer.writeToCurrentPage(edgeCount);
        _writer.writeToCurrentPage(pageCountPerDir);

        const auto dumpEdges = [&](std::span<const EdgeRecord> records) {
            const size_t remainder = edgeCount % Constants::COUNT_PER_PAGE;

            // EdgeRecord is four packed uint64 ID wrappers in field write
            // order (pinned in EdgeContainerDumpConstants.h), so pages of
            // records are written straight from the record array,
            // byte-identical to the per-field writes.
            const uint8_t* recordBytes = reinterpret_cast<const uint8_t*>(records.data());

            size_t offset = 0;
            for (size_t i = 0; i < pageCountPerDir; i++) {
                // New page
                _writer.nextPage();

                const bool isLastPage = (i == pageCountPerDir - 1);
                const size_t countInPage = isLastPage
                                             ? (remainder == 0 ? Constants::COUNT_PER_PAGE : remainder)
                                             : Constants::COUNT_PER_PAGE;

                // Header
                _writer.writeToCurrentPage(countInPage);

                // Data
                _writer.writeToCurrentPage(std::span {recordBytes + offset * Constants::RECORD_STRIDE, countInPage * Constants::RECORD_STRIDE});

                offset += countInPage;
            }
        };

        dumpEdges(edges.getOuts());
        dumpEdges(edges.getIns());

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGES, _writer.error().value());
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

}

