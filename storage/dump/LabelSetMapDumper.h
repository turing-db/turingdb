#pragma once

#include "metadata/LabelSetMap.h"
#include "GraphDumpHelper.h"

namespace db {

class LabelSetMapDumper {
private:
    struct PageMetadata {
        uint64_t labelsetCount {0};
        uint64_t charCount {0};
    };

public:
    explicit LabelSetMapDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    // Page header
    static constexpr size_t PAGE_HEADER_STRIDE = sizeof(uint64_t);

    // LabelSet stride
    static constexpr size_t LABELSET_STRIDE = LabelSet::IntegerSize * LabelSet::IntegerCount;

    // Avail space in data page
    static constexpr size_t PAGE_AVAIL = DumpConfig::PAGE_SIZE - PAGE_HEADER_STRIDE;

    // Count per page
    static constexpr size_t COUNT_PER_PAGE = PAGE_AVAIL / LABELSET_STRIDE;

    [[nodiscard]] DumpResult<void> dump(const LabelSetMap& labelsets) {
        GraphDumpHelper::writeFileHeader(_writer);

        const uint64_t labelsetCount = labelsets.getCount();
        const uint64_t pageCount = GraphDumpHelper::getPageCountForItems(
            labelsetCount, COUNT_PER_PAGE);

        // Metadata
        _writer.writeToCurrentPage((uint64_t)LabelSet::IntegerSize);
        _writer.writeToCurrentPage((uint64_t)LabelSet::IntegerCount);
        _writer.writeToCurrentPage(labelsetCount);
        _writer.writeToCurrentPage(pageCount);

        // Data
        const size_t remainder = labelsetCount % COUNT_PER_PAGE;

        size_t offset = 0;
        for (size_t i = 0; i < pageCount; i++) {
            // New page
            _writer.nextPage();

            const bool isLastPage = (i == pageCount - 1);
            const size_t countInPage = isLastPage
                                         ? (remainder == 0 ? COUNT_PER_PAGE : remainder)
                                         : COUNT_PER_PAGE;
            // Page header
            _writer.writeToCurrentPage(countInPage);

            // Page data
            for (size_t j = 0; j < countInPage; j++) {
                const auto& labelset = labelsets.getValue(offset);
                const auto* integers = labelset->data();
                for (size_t k = 0; k < LabelSet::IntegerCount; k++) {
                    _writer.writeToCurrentPage(integers[k]);
                }
                offset++;
            }
        }

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_LABELSETS, _writer.error().value());
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

};
