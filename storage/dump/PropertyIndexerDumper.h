#pragma once

#include "Profiler.h"
#include "indexers/PropertyIndexer.h"
#include "GraphDumpHelper.h"
#include "PropertyIndexerDumpConstants.h"

namespace db {

class PropertyIndexerDumper {
public:
    using Constants = PropertyIndexerDumperConstants;

    explicit PropertyIndexerDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const PropertyIndexer& indexer) {
        Profile profile {"PropertyIndexerDumper::dump"};
        _writer.nextPage();
        _writer.reserveSpace(Constants::PAGE_HEADER_STRIDE);

        uint64_t indexerCountInPage = 0;
        uint64_t pageCount = 1;

        auto* buffer = &_writer.buffer();
        for (const auto& [ptID, ptIndexer] : indexer) {
            const size_t stride = Constants::getPropTypeIndexerStride(ptIndexer);
            if (stride > Constants::PAGE_AVAIL) {
                return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROP_INDEXER);
            }

            if (buffer->avail() < stride) {
                // Fill page header
                buffer->patch(reinterpret_cast<const uint8_t*>(&indexerCountInPage), sizeof(uint64_t), 0);

                // Next page
                pageCount++;
                _writer.nextPage();
                _writer.reserveSpace(Constants::PAGE_HEADER_STRIDE);
                buffer = &_writer.buffer();
                indexerCountInPage = 0;
            }

            _writer.writeToCurrentPage(ptID.getValue());
            _writer.writeToCurrentPage(ptIndexer.size());

            for (const auto& [lset, info] : ptIndexer) {
                _writer.writeToCurrentPage(lset.getID().getValue());
                _writer.writeToCurrentPage((uint64_t)info.size());

                for (const auto& range : info) {
                    _writer.writeToCurrentPage((uint64_t)range._offset);
                    _writer.writeToCurrentPage((uint64_t)range._count);
                }
            }

            indexerCountInPage++;
        }

        buffer->patch(reinterpret_cast<const uint8_t*>(&indexerCountInPage), sizeof(uint64_t), 0);

        // Back to beginning to write metadata
        _writer.seek(0);
        GraphDumpHelper::writeFileHeader(_writer);
        _writer.writeToCurrentPage(indexer.size());
        _writer.writeToCurrentPage(pageCount);

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROP_INDEXER, _writer.error().value());
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

}

