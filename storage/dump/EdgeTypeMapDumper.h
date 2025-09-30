#pragma once

#include "metadata/EdgeTypeMap.h"
#include "GraphDumpHelper.h"

namespace db {

class EdgeTypeMapDumper {
private:
    struct PageMetadata {
        uint64_t edgeTypeCount {0};
        uint64_t charCount {0};
    };

public:
    explicit EdgeTypeMapDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const EdgeTypeMap& edgeTypes) {
        // Page metadata
        static constexpr size_t PAGE_HEADER_STRIDE = sizeof(uint64_t);

        // EdgeType stride = size of string
        static constexpr size_t EDGE_TYPE_BASE_STRIDE = sizeof(uint64_t);

        const size_t edgeTypeCount = edgeTypes.getCount();

        _writer.nextPage();
        _writer.reserveSpace(PAGE_HEADER_STRIDE);

        uint64_t countInPage = 0;
        uint64_t pageCount = 1;

        auto* buffer = &_writer.buffer();
        for (const auto& [i, name] : edgeTypes) {
            const uint64_t strsize = name->size();
            const size_t stride = EDGE_TYPE_BASE_STRIDE + strsize;

            if (buffer->avail() < stride) {
                // Fill page header
                buffer->patch(reinterpret_cast<const uint8_t*>(&countInPage), sizeof(uint64_t), 0);

                // Next page
                pageCount++;
                _writer.nextPage();
                _writer.reserveSpace(PAGE_HEADER_STRIDE);
                buffer = &_writer.buffer();
            }

            countInPage++;
            _writer.writeToCurrentPage(strsize);
            _writer.writeToCurrentPage(*name);
        }

        buffer->patch(reinterpret_cast<const uint8_t*>(&countInPage), sizeof(uint64_t), 0);

        // Back to beginning to write metadata
        _writer.seek(0);
        GraphDumpHelper::writeFileHeader(_writer);
        _writer.writeToCurrentPage(edgeTypeCount);
        _writer.writeToCurrentPage(pageCount);

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_TYPES);
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

};
