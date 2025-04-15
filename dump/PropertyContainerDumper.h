#pragma once

#include "properties/PropertyContainer.h"
#include "GraphDumpHelper.h"
#include "FilePageWriter.h"
#include "PropertyContainerDumpConstants.h"


namespace db {

template <TrivialSupportedType T>
class TrivialPropertyContainerDumper {
public:
    using Constants = TrivialPropertyContainerDumpConstants<T>;

    explicit TrivialPropertyContainerDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const TypedPropertyContainer<T>& props) {
        GraphDumpHelper::writeFileHeader(_writer);

        const uint64_t propCount = props.size();

        // Page counts
        const uint64_t idPageCount = GraphDumpHelper::getPageCountForItems(
            propCount, Constants::ID_COUNT_PER_PAGE);
        const uint64_t valuePageCount = GraphDumpHelper::getPageCountForItems(
            propCount, Constants::VALUE_COUNT_PER_PAGE);

        // Metadata
        _writer.writeToCurrentPage(props.getValueType());
        _writer.writeToCurrentPage(propCount);
        _writer.writeToCurrentPage(idPageCount);
        _writer.writeToCurrentPage(valuePageCount);

        {
            // IDs
            const size_t remainder = propCount % Constants::ID_COUNT_PER_PAGE;
            const auto& ids = props.ids();

            size_t offset = 0;
            for (size_t i = 0; i < idPageCount; i++) {
                // New page
                _writer.nextPage();

                const bool isLastPage = (i == idPageCount - 1);
                const size_t countInPage = isLastPage
                                             ? (remainder == 0 ? Constants::ID_COUNT_PER_PAGE : remainder)
                                             : Constants::ID_COUNT_PER_PAGE;
                const std::span idSpan = std::span {ids}.subspan(offset, countInPage);
                offset += countInPage;

                // Header
                _writer.writeToCurrentPage(countInPage);

                // Data
                for (const auto& id : idSpan) {
                    _writer.writeToCurrentPage(id.getValue());
                }
            }
        }

        {
            // Values
            const size_t remainder = propCount % Constants::VALUE_COUNT_PER_PAGE;
            const auto& values = props.all();

            size_t offset = 0;
            for (size_t i = 0; i < valuePageCount; i++) {
                // New page
                _writer.nextPage();

                const bool isLastPage = (i == valuePageCount - 1);
                const size_t countInPage = isLastPage
                                             ? (remainder == 0 ? Constants::VALUE_COUNT_PER_PAGE : remainder)
                                             : Constants::VALUE_COUNT_PER_PAGE;
                const std::span valueSpan = std::span {values}.subspan(offset, countInPage);
                offset += countInPage;


                // Header
                _writer.writeToCurrentPage(countInPage);

                // Data
                _writer.writeToCurrentPage(valueSpan);
            }
        }

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS, _writer.error().value());
        }

        return {};
    }


private:
    fs::FilePageWriter& _writer;
};

class StringPropertyContainerDumper {
public:
    using Constants = StringPropertyContainerDumpConstants;

    explicit StringPropertyContainerDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const TypedPropertyContainer<types::String>& props) {
        const auto& buckets = props.getRawContainer();
        const uint64_t propCount = props.size();
        const uint64_t bucketCount = buckets.bucketCount();

        // Page counts
        const uint64_t idPageCount = GraphDumpHelper::getPageCountForItems(
            propCount, Constants::ID_COUNT_PER_PAGE);

        const uint64_t bucketPageCount = GraphDumpHelper::getPageCountForItems(
            bucketCount, Constants::BUCKET_COUNT_PER_PAGE);

        uint64_t limitsPageCount = 1;

        // Skip first page, it will be written at the end

        {
            // IDs
            const size_t remainder = propCount % Constants::ID_COUNT_PER_PAGE;
            std::span ids {props.ids()};

            size_t offset = 0;
            for (size_t i = 0; i < idPageCount; i++) {
                // New page
                _writer.nextPage();

                const bool isLastPage = (i == idPageCount - 1);
                const size_t countInPage = isLastPage
                                             ? (remainder == 0 ? Constants::ID_COUNT_PER_PAGE : remainder)
                                             : Constants::ID_COUNT_PER_PAGE;
                const std::span pageIDs = ids.subspan(offset, countInPage);
                offset += countInPage;

                // Header
                _writer.writeToCurrentPage(countInPage);

                // Data
                for (const auto& id : pageIDs) {
                    _writer.writeToCurrentPage(id.getValue());
                }
            }
        }

        {
            // Buckets
            const size_t remainder = bucketCount % Constants::BUCKET_COUNT_PER_PAGE;

            size_t offset = 0;
            for (size_t i = 0; i < bucketPageCount; i++) {
                // New page
                _writer.nextPage();

                const bool isLastPage = (i == bucketPageCount - 1);
                const size_t countInPage = isLastPage
                                             ? (remainder == 0 ? Constants::BUCKET_COUNT_PER_PAGE : remainder)
                                             : Constants::BUCKET_COUNT_PER_PAGE;

                // Header
                _writer.writeToCurrentPage(countInPage);

                for (size_t j = 0; j < countInPage; j++) {
                    const auto& bucket = buckets.bucket(offset);

                    // Data
                    _writer.writeToCurrentPage(bucket.span());
                    offset++;
                }
            }
        }

        {
            // Limits
            size_t blockCountInPage = 0;

            _writer.nextPage();
            _writer.reserveSpace(Constants::LIMIT_PAGE_HEADER_STRIDE);

            auto* buffer = &_writer.buffer();


            for (uint64_t i = 0; i < buckets.bucketCount(); i++) {
                const auto& bucket = buckets.bucket(i);
                const std::span limits = bucket.limits();
                uint32_t remainingStrCount = bucket.strCount();

                size_t offset = 0;

                while (remainingStrCount != 0) {
                    if (buffer->avail() < (ssize_t)Constants::MIN_BLOCK_STRIDE) {
                        // Not enough space to store a block of limits
                        // Fill page header
                        buffer->patch(reinterpret_cast<const uint8_t*>(&blockCountInPage), sizeof(uint64_t), 0);

                        // Next page
                        limitsPageCount++;
                        _writer.nextPage();
                        _writer.reserveSpace(Constants::LIMIT_PAGE_HEADER_STRIDE);
                        buffer = &_writer.buffer();
                        blockCountInPage = 0;
                    }

                    const ssize_t maxStride = remainingStrCount * Constants::LIMIT_STRIDE;
                    const ssize_t availSpace = buffer->avail() - Constants::LIMIT_BLOCK_HEADER_STRIDE;
                    const size_t limitsStride = std::min(availSpace, maxStride);

                    const uint32_t blockStrCount = limitsStride / Constants::LIMIT_STRIDE;
                    const std::span blockLimits = limits.subspan(offset, blockStrCount);
                    remainingStrCount -= blockStrCount;

                    _writer.writeToCurrentPage(i);
                    _writer.writeToCurrentPage(blockStrCount);

                    for (const auto& lim : blockLimits) {
                        _writer.writeToCurrentPage(lim._offset);
                        _writer.writeToCurrentPage(lim._count);
                    }

                    offset += blockStrCount;
                    blockCountInPage++;
                }
            }

            buffer->patch(reinterpret_cast<const uint8_t*>(&blockCountInPage), sizeof(uint64_t), 0);
        }

        // Back to beginning to write metadata
        _writer.seek(0);

        GraphDumpHelper::writeFileHeader(_writer);
        _writer.writeToCurrentPage(ValueType::String);
        _writer.writeToCurrentPage(propCount);
        _writer.writeToCurrentPage(idPageCount);
        _writer.writeToCurrentPage(bucketPageCount);
        _writer.writeToCurrentPage(limitsPageCount);

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS, _writer.error().value());
        }

        return {};
    }


private:
    fs::FilePageWriter& _writer;
};

}
