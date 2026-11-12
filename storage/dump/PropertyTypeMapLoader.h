#pragma once

#include "metadata/PropertyTypeMap.h"
#include "DumpResult.h"
#include "FilePageReader.h"
#include "GraphDumpHelper.h"

namespace db {

class PropertyTypeMapLoader {
public:
    static constexpr size_t METADATA_PAGE_STRIDE = DumpConfig::FILE_HEADER_STRIDE
                                                 + sizeof(uint64_t)  // PropertyType count
                                                 + sizeof(uint64_t); // Page count

    static constexpr size_t PROP_TYPE_BASE_STRIDE = sizeof(ValueType) // Value type
                                                  + sizeof(uint64_t); // String size

    static constexpr size_t EMBEDDING_DIMENSION_STRIDE = sizeof(bool)      // Has an embedding dimension
                                                       + sizeof(uint64_t); // Embedding dimension

    explicit PropertyTypeMapLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<void> load(PropertyTypeMap& propTypes, bool& holdsEmbeddingDimensions) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES, _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        if (it.remainingBytes() < METADATA_PAGE_STRIDE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES);
        }

        const auto version = GraphDumpHelper::checkFileHeader(it);
        if (!version) {
            return version.get_unexpected();
        }

        holdsEmbeddingDimensions = version.value() >= DumpConfig::EMBEDDING_DIMENSION_VERSION;

        const size_t propTypeStride = holdsEmbeddingDimensions
                                    ? PROP_TYPE_BASE_STRIDE + EMBEDDING_DIMENSION_STRIDE
                                    : PROP_TYPE_BASE_STRIDE;

        const uint64_t propTypeCount = it.get<uint64_t>();
        const uint64_t pageCount = it.get<uint64_t>();

        for (size_t i = 0; i < pageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES);
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() < DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                if (it.remainingBytes() < propTypeStride) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES);
                }

                const ValueType valueType = it.get<ValueType>();

                bool hasEmbeddingDimension = false;
                uint64_t embeddingDimension = 0;
                if (holdsEmbeddingDimensions) {
                    hasEmbeddingDimension = it.get<bool>();
                    embeddingDimension = it.get<uint64_t>();
                }

                const uint64_t strsize = it.get<uint64_t>();

                if (it.remainingBytes() < strsize) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES);
                }

                const std::string_view name = it.get<char>(strsize);
                const PropertyType pt = propTypes.getOrCreate(std::string {name}, valueType);

                if (hasEmbeddingDimension) {
                    propTypes.getOrSetEmbeddingDimension(pt._id, embeddingDimension);
                }
            }
        }

        if (propTypeCount != propTypes.getCount()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES);
        }

        return {};
    }

private:
    fs::FilePageReader& _reader;
};

}
