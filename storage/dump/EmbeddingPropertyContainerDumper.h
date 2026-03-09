#pragma once

#include "Profiler.h"
#include "properties/PropertyContainer.h"
#include "GraphDumpHelper.h"
#include "FilePageWriter.h"
#include "PropertyContainerDumpConstants.h"

namespace db {

class EmbeddingPropertyContainerDumper {
public:
    using Constants = EmbeddingPropertyContainerDumpConstants;

    explicit EmbeddingPropertyContainerDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const TypedPropertyContainer<types::Embedding>& props) {
        Profile profile("EmbeddingPropertyContainerDumper::dump");

        const auto& container = props.getRawContainer();
        const uint64_t propCount = props.size();
        const uint32_t dimension = container.dimension();

        // Total floats = propCount * dimension
        const uint64_t totalFloats = (uint64_t)propCount * dimension;

        // Page counts
        const uint64_t idPageCount = GraphDumpHelper::getPageCountForItems(
            propCount, Constants::ID_COUNT_PER_PAGE);
        const uint64_t dataPageCount = GraphDumpHelper::getPageCountForItems(
            totalFloats, Constants::FLOAT_COUNT_PER_PAGE);

        // File header + metadata
        GraphDumpHelper::writeFileHeader(_writer);
        _writer.writeToCurrentPage(ValueType::Embedding);
        _writer.writeToCurrentPage(static_cast<uint8_t>(EmbeddingPrecision::Float32));
        _writer.writeToCurrentPage(dimension);
        _writer.writeToCurrentPage(propCount);
        _writer.writeToCurrentPage(idPageCount);
        _writer.writeToCurrentPage(dataPageCount);

        {
            // IDs
            const size_t remainder = propCount % Constants::ID_COUNT_PER_PAGE;
            const auto& ids = props.ids();

            size_t offset = 0;
            for (size_t i = 0; i < idPageCount; i++) {
                _writer.nextPage();

                const bool isLastPage = (i == idPageCount - 1);
                const size_t countInPage = isLastPage
                                             ? (remainder == 0 ? Constants::ID_COUNT_PER_PAGE : remainder)
                                             : Constants::ID_COUNT_PER_PAGE;
                const std::span idSpan = std::span {ids}.subspan(offset, countInPage);
                offset += countInPage;

                _writer.writeToCurrentPage(countInPage);

                for (const auto& id : idSpan) {
                    _writer.writeToCurrentPage(id.getValue());
                }
            }
        }

        {
            // Data (raw floats, embedding by embedding)
            const size_t remainder = totalFloats % Constants::FLOAT_COUNT_PER_PAGE;

            size_t embeddingIdx = 0;
            size_t floatOffsetInEmbedding = 0;
            for (size_t i = 0; i < dataPageCount; i++) {
                _writer.nextPage();

                const bool isLastPage = (i == dataPageCount - 1);
                const size_t floatsInPage = isLastPage
                                              ? (remainder == 0 ? Constants::FLOAT_COUNT_PER_PAGE : remainder)
                                              : Constants::FLOAT_COUNT_PER_PAGE;

                _writer.writeToCurrentPage(floatsInPage);

                size_t pageFloatsWritten = 0;
                while (pageFloatsWritten < floatsInPage) {
                    const auto& view = container.getView(embeddingIdx);
                    const size_t remainingInEmbedding = dimension - floatOffsetInEmbedding;
                    const size_t remainingInPage = floatsInPage - pageFloatsWritten;
                    const size_t toWrite = std::min(remainingInEmbedding, remainingInPage);

                    const std::span<const float> slice {
                        view.data() + floatOffsetInEmbedding,
                        toWrite
                    };
                    _writer.writeToCurrentPage(slice);

                    pageFloatsWritten += toWrite;
                    floatOffsetInEmbedding += toWrite;

                    if (floatOffsetInEmbedding == dimension) {
                        embeddingIdx++;
                        floatOffsetInEmbedding = 0;
                    }
                }

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

}
