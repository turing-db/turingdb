#pragma once

#include "metadata/EdgeTypeMap.h"
#include "FilePageReader.h"
#include "GraphDumpHelper.h"

namespace db {

class EdgeTypeMapLoader {
public:
    static constexpr size_t METADATA_PAGE_STRIDE = DumpConfig::FILE_HEADER_STRIDE
                                                 + sizeof(uint64_t)  // EdgeType count
                                                 + sizeof(uint64_t); // Page count

    static constexpr size_t EDGE_TYPE_BASE_STRIDE = sizeof(uint64_t); // String size

    explicit EdgeTypeMapLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<void> load(EdgeTypeMap& edgeTypes) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES,
                                     _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        if (it.remainingBytes() < METADATA_PAGE_STRIDE) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES);
        }

        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        const uint64_t edgeTypeCount = it.get<uint64_t>();
        const uint64_t pageCount = it.get<uint64_t>();

        for (size_t i = 0; i < pageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES,
                                         _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() < DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                if (it.remainingBytes() < EDGE_TYPE_BASE_STRIDE) {
                    return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES);
                }

                const uint64_t strsize = it.get<uint64_t>();

                if (it.remainingBytes() < strsize) {
                    return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES);
                }

                const std::string_view name = it.get<char>(strsize);
                edgeTypes.getOrCreate(std::string {name});
            }
        }

        if (edgeTypeCount != edgeTypes.getCount()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES);
        }

        return {};
    }

private:
    fs::FilePageReader& _reader;
};

}
