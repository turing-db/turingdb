#pragma once

#include "metadata/PropertyTypeMap.h"
#include "GraphDumpHelper.h"

namespace db {

class PropertyTypeMapDumper {
public:
    explicit PropertyTypeMapDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const PropertyTypeMap& propTypes) {
        // Page metadata
        static constexpr size_t PAGE_HEADER_STRIDE = sizeof(uint64_t);

        // PropertyType stride = size of string
        static constexpr size_t PROPERTY_TYPE_BASE_STRIDE = sizeof(ValueType) + sizeof(uint64_t);

        const uint64_t propTypeCount = propTypes.getCount();

        _writer.nextPage();
        _writer.reserveSpace(PAGE_HEADER_STRIDE);

        uint64_t countInPage = 0;
        uint64_t pageCount = 1;

        auto* buffer = &_writer.buffer();
        for (const auto& [pt, name] : propTypes) {
            const uint64_t strsize = name->size();
            const size_t stride = PROPERTY_TYPE_BASE_STRIDE + strsize;

            if (buffer->avail() < stride) {
                // Fill page header
                buffer->patch(reinterpret_cast<const uint8_t*>(&countInPage), sizeof(uint64_t), 0);

                // Next page
                pageCount++;
                _writer.nextPage();
                _writer.reserveSpace(PAGE_HEADER_STRIDE);
                buffer = &_writer.buffer();
                countInPage = 0;
            }

            _writer.writeToCurrentPage(pt._valueType);
            _writer.writeToCurrentPage(strsize);
            _writer.writeToCurrentPage(*name);
            countInPage++;
        }

        buffer->patch(reinterpret_cast<const uint8_t*>(&countInPage), sizeof(uint64_t), 0);

        // Back to beginning to write metadata
        _writer.seek(0);
        GraphDumpHelper::writeFileHeader(_writer);
        _writer.writeToCurrentPage(propTypeCount);
        _writer.writeToCurrentPage(pageCount);

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROP_TYPES, _writer.error().value());
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

};
