#include "PropertyContainerDumper.h"

#include <stdint.h>
#include <algorithm>
#include <span>

#include "GraphDumpHelper.h"
#include "PropertyContainerDumpConstants.h"

#include "list/EncodedList.h"

using namespace db;

ListPropertyContainerDumper::ListPropertyContainerDumper(fs::FilePageWriter& writer)
    : _writer(writer)
{
}

ListPropertyContainerDumper::~ListPropertyContainerDumper() {
}

DumpResult<void> ListPropertyContainerDumper::dump(const TypedPropertyContainer<types::List>& props) {
    Profile profile("ListPropertyContainerDumper::dump");

    const uint64_t propCount = props.size();

    // A list has no fixed width, so the values go out as one byte stream of
    // [length][encoded list] records rather than as a strided array. The stream is built
    // up front because its total size is part of the metadata page, which is written
    // first.
    std::vector<std::byte> stream;
    for (const ListView list : props.all()) {
        const EncodedList encoded(list);
        const std::span<const std::byte> bytes = encoded.bytes();

        const uint64_t length = bytes.size();
        const std::byte* lengthBytes = reinterpret_cast<const std::byte*>(&length);
        stream.insert(stream.end(), lengthBytes, lengthBytes + sizeof(length));
        stream.insert(stream.end(), bytes.begin(), bytes.end());
    }

    const uint64_t totalBytes = stream.size();

    const size_t idStride = sizeof(EntityID::Type);
    const size_t headerStride = sizeof(uint64_t);
    const size_t idCountPerPage = (DumpConfig::PAGE_SIZE - headerStride) / idStride;
    const size_t bytesPerPage = DumpConfig::PAGE_SIZE - headerStride;

    const uint64_t idPageCount = GraphDumpHelper::getPageCountForItems(propCount, idCountPerPage);
    const uint64_t bytePageCount = GraphDumpHelper::getPageCountForItems(totalBytes, bytesPerPage);

    GraphDumpHelper::writeFileHeader(_writer);
    _writer.writeToCurrentPage(ValueType::List);
    _writer.writeToCurrentPage(propCount);
    _writer.writeToCurrentPage(totalBytes);
    _writer.writeToCurrentPage(idPageCount);
    _writer.writeToCurrentPage(bytePageCount);

    {
        // IDs
        const size_t remainder = propCount % idCountPerPage;
        const auto& ids = props.ids();

        // See the TrivialPropertyContainerDumper IDs section: pages of IDs are written
        // straight from the ID array, layout pinned in PropertyContainerDumpConstants.h.
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
        // Encoded lists
        const size_t remainder = totalBytes % bytesPerPage;

        size_t offset = 0;
        for (size_t i = 0; i < bytePageCount; i++) {
            _writer.nextPage();

            const bool isLastPage = (i == bytePageCount - 1);
            const size_t countInPage = isLastPage
                                         ? (remainder == 0 ? bytesPerPage : remainder)
                                         : bytesPerPage;

            _writer.writeToCurrentPage(countInPage);
            _writer.writeToCurrentPage(std::span {reinterpret_cast<const uint8_t*>(stream.data()) + offset, countInPage});

            offset += countInPage;
        }
    }

    _writer.finish();

    if (_writer.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS, _writer.error().value());
    }

    return {};
}
