#pragma once

#include <memory>

#include "Profiler.h"
#include "properties/PropertyContainer.h"
#include "FilePageReader.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "PropertyContainerDumpConstants.h"

namespace db {

class EmbeddingPropertyContainerLoader {
public:
    using Constants = EmbeddingPropertyContainerDumpConstants;

    explicit EmbeddingPropertyContainerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<PropertyContainer>> load() {
        Profile profile("EmbeddingPropertyContainerLoader::load");

        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
        }

        auto it = _reader.begin();

        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }

        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        [[maybe_unused]] const ValueType valueType = it.get<ValueType>();
        const uint8_t precisionByte = it.get<uint8_t>();
        const uint32_t dimension = it.get<uint32_t>();
        const uint64_t propCount = it.get<uint64_t>();
        const uint64_t idPageCount = it.get<uint64_t>();
        const uint64_t dataPageCount = it.get<uint64_t>();

        bioassert(precisionByte == static_cast<uint8_t>(EmbeddingPrecision::Float32),
                  "Only Float32 precision is supported");

        EmbeddingPropertyConfig config {dimension, EmbeddingPrecision::Float32};
        auto* container = new TypedPropertyContainer<types::Embedding>(config);
        container->_ids.resize(propCount);

        // Loading IDs
        size_t offset = 0;

        for (size_t i = 0; i < idPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
            }

            it = _reader.begin();

            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                container->_ids[j + offset] = it.get<EntityID::Type>();
            }

            offset += countInPage;
        }

        // Loading data (raw floats)
        const uint64_t totalFloats = (uint64_t)propCount * dimension;
        std::vector<float> allFloats(totalFloats);
        size_t floatOffset = 0;

        for (size_t i = 0; i < dataPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
            }

            it = _reader.begin();

            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            const size_t floatsInPage = it.get<uint64_t>();

            for (size_t j = 0; j < floatsInPage; j++) {
                allFloats[floatOffset + j] = it.get<float>();
            }

            floatOffset += floatsInPage;
        }

        // Rebuild container from raw floats
        // Clear the default bucket that was created in the constructor
        container->_values.clear();

        const uint32_t embeddingsPerBucket = EmbeddingBucket::BUCKET_FLOATS / dimension;
        size_t embeddingsLoaded = 0;

        while (embeddingsLoaded < propCount) {
            const size_t remaining = propCount - embeddingsLoaded;
            const size_t batchSize = std::min((size_t)embeddingsPerBucket, remaining);

            EmbeddingBucket bucket(dimension);
            for (size_t i = 0; i < batchSize; i++) {
                const float* ptr = allFloats.data() + (embeddingsLoaded + i) * dimension;
                bucket.alloc({ptr, dimension});
            }

            container->_values.addBucket(std::move(bucket));
            embeddingsLoaded += batchSize;
        }

        // Reconstruct entity index map via sort()
        container->sort();

        return {std::unique_ptr<PropertyContainer> {container}};
    }

private:
    fs::FilePageReader& _reader;
};

}
