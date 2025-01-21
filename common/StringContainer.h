#pragma once

#include <cstring>
#include <span>
#include <vector>
#include <string_view>

#include "BioAssert.h"

class StringContainer {
public:
    static constexpr size_t BUCKET_SIZE = 512ul * 1024;
    using ViewVector = std::vector<std::string_view>;

    StringContainer()
        : _buckets(1),
          _countsPerBucket({0})
    {
        _buckets.back().resize(BUCKET_SIZE);
    }

    ~StringContainer() = default;
    StringContainer(StringContainer&& other) noexcept = default;
    StringContainer& operator=(StringContainer&& other) noexcept = default;
    StringContainer(const StringContainer& other) = delete;
    StringContainer& operator=(const StringContainer& other) = delete;

    void alloc(std::string_view content) {
        if (_remainingSize < content.size()) {
            if (content.size() > BUCKET_SIZE) {
                auto& oneValueBucket = _buckets.emplace_back();
                _countsPerBucket.push_back(1);
                oneValueBucket.resize(content.size());
                std::memcpy(oneValueBucket.data(), content.data(), content.size());
                _views.push_back({oneValueBucket.data(), content.size()});

                _remainingSize = 0;
                return;
            } else {
                _remainingSize = BUCKET_SIZE;
                auto& bucket = _buckets.emplace_back();
                bucket.resize(BUCKET_SIZE);
                _countsPerBucket.push_back(0);
            }
        }

        auto& bucket = _buckets.back();
        _countsPerBucket.back()++;
        const size_t offset = bucket.size() - _remainingSize;
        _remainingSize -= content.size();

        std::memcpy(bucket.data() + offset, content.data(), content.size());
        _views.push_back({bucket.data() + offset, content.size()});
    }


    const std::string_view& getView(size_t index) const {
        msgbioassert(index < _views.size(), "String index invalid");
        return _views[index];
    }

    size_t size() const { return _views.size(); }

    const ViewVector& get() const { return _views; }
    const std::vector<size_t>& getCountsPerBucket() const { return _countsPerBucket; }
    size_t bucketCount() const { return _buckets.size(); }
    size_t countInBucket(size_t bucketI) const { return _countsPerBucket[bucketI]; }
    size_t remainingSpaceInBucket() const { return _remainingSize; }

    std::span<const char> bucket(size_t i) const {
        return _buckets[i];
    }

    ViewVector::const_iterator begin() const { return _views.begin(); }
    ViewVector::const_iterator end() const { return _views.end(); }

private:
    std::vector<std::vector<char>> _buckets;
    std::vector<size_t> _countsPerBucket;
    size_t _remainingSize = BUCKET_SIZE;
    ViewVector _views;
};
