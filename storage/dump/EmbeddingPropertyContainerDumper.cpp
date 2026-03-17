#include "PropertyContainerDumper.h"

#include "GraphDumpHelper.h"
#include "PropertyContainerDumpConstants.h"

using namespace db;

EmbeddingPropertyContainerDumper::EmbeddingPropertyContainerDumper(fs::FilePageWriter& writer)
    : _writer(writer)
{
}

EmbeddingPropertyContainerDumper::~EmbeddingPropertyContainerDumper() {
}

DumpResult<void> EmbeddingPropertyContainerDumper::dump(const TypedPropertyContainer<types::Embedding>& props) {
    Profile profile("EmbeddingPropertyContainerDumper::dump");

    const auto& container = props.getRawContainer();
    const uint64_t propCount = props.size();
    const uint64_t dimension = container.getDimension();
    const uint64_t totalFloats = propCount * dimension;

    const size_t idStride = sizeof(EntityID::Type);
    const size_t idHeaderStride = sizeof(uint64_t);
    const size_t idPageAvail = DumpConfig::PAGE_SIZE - idHeaderStride;
    const size_t idCountPerPage = idPageAvail / idStride;

    const size_t floatHeaderStride = sizeof(uint64_t);
    const size_t floatPageAvail = DumpConfig::PAGE_SIZE - floatHeaderStride;
    const size_t floatsPerPage = floatPageAvail / sizeof(float);

    const uint64_t idPageCount = GraphDumpHelper::getPageCountForItems(propCount, idCountPerPage);
    const uint64_t floatPageCount = GraphDumpHelper::getPageCountForItems(totalFloats, floatsPerPage);

    // Metadata page
    GraphDumpHelper::writeFileHeader(_writer);
    _writer.writeToCurrentPage(ValueType::Embedding);
    _writer.writeToCurrentPage(propCount);
    _writer.writeToCurrentPage(dimension);
    _writer.writeToCurrentPage(idPageCount);
    _writer.writeToCurrentPage(floatPageCount);

    {
        // IDs
        const size_t remainder = propCount % idCountPerPage;
        const auto& ids = props.ids();

        size_t offset = 0;
        for (size_t i = 0; i < idPageCount; i++) {
            _writer.nextPage();

            const bool isLastPage = (i == idPageCount - 1);
            const size_t countInPage = isLastPage
                                         ? (remainder == 0 ? idCountPerPage : remainder)
                                         : idCountPerPage;
            const std::span idSpan = std::span{ids}.subspan(offset, countInPage);
            offset += countInPage;

            _writer.writeToCurrentPage(countInPage);
            for (const auto& id : idSpan) {
                _writer.writeToCurrentPage(id.getValue());
            }
        }
    }

    {
        // Float data — written as a flat stream across pages
        const size_t remainder = totalFloats % floatsPerPage;
        size_t floatOffset = 0;

        for (size_t i = 0; i < floatPageCount; i++) {
            _writer.nextPage();

            const bool isLastPage = (i == floatPageCount - 1);
            const size_t countInPage = isLastPage
                                         ? (remainder == 0 ? floatsPerPage : remainder)
                                         : floatsPerPage;

            _writer.writeToCurrentPage(countInPage);

            for (size_t j = 0; j < countInPage; j++) {
                const size_t embIdx = floatOffset / dimension;
                const size_t floatIdx = floatOffset % dimension;
                const std::span<const float> view = container.getView(embIdx);
                _writer.writeToCurrentPage(view[floatIdx]);
                floatOffset++;
            }
        }
    }

    _writer.finish();

    if (_writer.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS, _writer.error().value());
    }

    return {};
}
