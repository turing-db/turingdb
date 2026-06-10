#include "PropertyContainerDumper.h"

#include <stdint.h>
#include <algorithm>
#include <span>

#include "GraphDumpHelper.h"
#include "PropertyContainerDumpConstants.h"

#include "BioAssert.h"

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

        // See the TrivialPropertyContainerDumper IDs section: pages of IDs
        // are written straight from the ID array, layout pinned in
        // PropertyContainerDumpConstants.h.
        const uint8_t* idBytes = reinterpret_cast<const uint8_t*>(ids.data());

        size_t offset = 0;
        for (size_t i = 0; i < idPageCount; i++) {
            _writer.nextPage();

            const bool isLastPage = (i == idPageCount - 1);
            const size_t countInPage = isLastPage
                                         ? (remainder == 0 ? idCountPerPage : remainder)
                                         : idCountPerPage;

            _writer.writeToCurrentPage(countInPage);
            _writer.writeToCurrentPage(std::span {idBytes + offset * idStride, countInPage * idStride});

            offset += countInPage;
        }
    }

    {
        // Float data — written as a flat stream across pages
        const size_t remainder = totalFloats % floatsPerPage;
        size_t floatOffset = 0;

        // The stream is sliced straight out of the container's buckets: views
        // are stored in allocation order at dump time (alloc, the loader and
        // sort() all fill buckets sequentially), so a bucket's used span is
        // exactly the floats of its views in index order. A run is capped by
        // the floats remaining in the page (to preserve the page layout) and
        // by the bucket end, so a write never touches more than one
        // allocation. Float order — and therefore the byte output — is
        // unchanged from per-float writes.
        size_t bucketIndex = 0;
        size_t bucketFirstView = 0;

        for (size_t i = 0; i < floatPageCount; i++) {
            _writer.nextPage();

            const bool isLastPage = (i == floatPageCount - 1);
            const size_t countInPage = isLastPage
                                         ? (remainder == 0 ? floatsPerPage : remainder)
                                         : floatsPerPage;

            _writer.writeToCurrentPage(countInPage);

            size_t toWrite = countInPage;
            while (toWrite > 0) {
                const size_t embIdx = floatOffset / dimension;
                const size_t floatIdx = floatOffset % dimension;

                while (embIdx >= bucketFirstView + container.bucket(bucketIndex).getEmbeddingCount()) {
                    bucketFirstView += container.bucket(bucketIndex).getEmbeddingCount();
                    bucketIndex++;
                    bioassert(bucketIndex < container.bucketCount(), "Embedding bucket walk ran past the last bucket");
                }

                const std::span<const float> bucketSpan = container.bucket(bucketIndex).span();
                const size_t bucketFloatIndex = (embIdx - bucketFirstView) * dimension + floatIdx;

                // Verify the allocation-order invariant the slicing relies on.
                const std::span<const float> view = container.getView(embIdx);
                bioassert(view.size() == dimension, "Embedding view size must match the container dimension");
                bioassert(view.data() == bucketSpan.data() + (embIdx - bucketFirstView) * dimension, "Embedding views must be stored in allocation order");

                const size_t n = std::min(bucketSpan.size() - bucketFloatIndex, toWrite);
                _writer.writeToCurrentPage(std::span {bucketSpan.data() + bucketFloatIndex, n});

                floatOffset += n;
                toWrite -= n;
            }
        }
    }

    _writer.finish();

    if (_writer.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS, _writer.error().value());
    }

    return {};
}
