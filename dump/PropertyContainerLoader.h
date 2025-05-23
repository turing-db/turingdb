#pragma once

#include <memory>

#include "Profiler.h"
#include "properties/PropertyContainer.h"
#include "FilePageReader.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "PropertyContainerDumpConstants.h"

namespace db {

template <SupportedType T>
class TrivialPropertyContainerLoader {
public:
    using Constants = TrivialPropertyContainerDumpConstants<T>;

    explicit TrivialPropertyContainerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<PropertyContainer>> load() {
        Profile profile {"TrivialPropertyContainerLoader::load"};

        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        // Check if we received a full page
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }

        // Check file header
        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        [[maybe_unused]] const ValueType valueType = it.get<ValueType>();
        const uint64_t propCount = it.get<uint64_t>();
        const uint64_t idPageCount = it.get<uint64_t>();
        const uint64_t valuePageCount = it.get<uint64_t>();

        auto* container = new TypedPropertyContainer<T>;
        container->_ids.resize(propCount);
        container->_values.resize(propCount);

        // Loading ids
        size_t offset = 0;

        for (size_t i = 0; i < idPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                container->_ids[j + offset] = it.get<EntityID::Type>();
            }

            offset += countInPage;
        }

        // Loading values
        offset = 0;

        for (size_t i = 0; i < valuePageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                container->_values[j + offset] = it.get<typename T::Primitive>();
            }

            offset += countInPage;
        }

        return {std::unique_ptr<PropertyContainer> {container}};
    }

private:
    fs::FilePageReader& _reader;
};

class StringPropertyContainerLoader {
public:
    using Constants = StringPropertyContainerDumpConstants;

    explicit StringPropertyContainerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<PropertyContainer>> load() {
        Profile profile {"StringPropertyContainerLoader::load"};

        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        // Check if we received a full page
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }

        // Check file header
        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        [[maybe_unused]] const ValueType valueType = it.get<ValueType>();
        const uint64_t propCount = it.get<uint64_t>();
        const uint64_t idPageCount = it.get<uint64_t>();
        const uint64_t bucketPageCount = it.get<uint64_t>();
        const uint64_t limitsPageCount = it.get<uint64_t>();

        auto* container = new TypedPropertyContainer<types::String>;
        container->_ids.resize(propCount);
        auto& buckets = container->_values;

        std::vector<std::vector<char>> rawBuckets;
        std::vector<std::vector<StringBucket::StringLimits>> rawLimits;

        // Loading ids
        size_t offset = 0;

        for (size_t i = 0; i < idPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                container->_ids[j + offset] = it.get<EntityID::Type>();
            }

            offset += countInPage;
        }

        // Loading buckets
        for (size_t i = 0; i < bucketPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            const size_t countInPage = it.get<uint64_t>();

            if (countInPage * Constants::BUCKET_STRIDE > Constants::BUCKET_PAGE_AVAIL) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            for (size_t j = 0; j < countInPage; j++) {
                const std::span chars = it.get<char>(StringBucket::BUCKET_SIZE);
                auto& bucket = rawBuckets.emplace_back();
                bucket.resize(StringBucket::BUCKET_SIZE);
                std::memcpy(bucket.data(), chars.data(), chars.size());
            }
        }

        // Loading string limits
        for (size_t i = 0; i < limitsPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS,
                                         _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            const size_t blockCountInPage = it.get<uint64_t>();

            for (size_t j = 0; j < blockCountInPage; j++) {
                const uint64_t bucketIndex = it.get<uint64_t>();
                const uint32_t blockStrCount = it.get<uint32_t>();

                // If did not change bucket, retrieve previous limit container
                // else, push new limit container corresponding to new bucket
                auto& limits = bucketIndex < rawLimits.size()
                                 ? rawLimits.back()
                                 : rawLimits.emplace_back();

                const size_t prevSize = limits.size();
                limits.resize(prevSize + blockStrCount);

                for (size_t k = 0; k < blockStrCount; k++) {
                    auto& lim = limits[k + prevSize];
                    lim._offset = it.get<uint32_t>();
                    lim._count = it.get<uint32_t>();
                }
            }
        }

        if (rawBuckets.size() != rawLimits.size()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }

        for (size_t i = 0; i < rawBuckets.size(); i++) {
            if (!buckets.addBucket(std::move(rawBuckets[i]), std::move(rawLimits[i]))) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }
        }

        return {std::unique_ptr<PropertyContainer> {container}};
    }

private:
    fs::FilePageReader& _reader;
};
}
