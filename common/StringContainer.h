#pragma once

#include <cstring>
#include <vector>
#include <string_view>

#include "BioAssert.h"

class StringContainer {
public:
    static constexpr size_t BUCKET_SIZE = 2ul * 1024;
    using ViewVector = std::vector<std::string_view>;

    StringContainer()
        : _buckets(1)
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
                oneValueBucket.resize(content.size());
                std::memcpy(oneValueBucket.data(), content.data(), content.size());
                _views.push_back({oneValueBucket.data(), content.size()});

                _remainingSize = 0;
                return;
            } else {
                _remainingSize = BUCKET_SIZE;
                auto& bucket = _buckets.emplace_back();
                bucket.resize(BUCKET_SIZE);
            }
        }

        auto& bucket = _buckets.back();
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

    ViewVector::const_iterator begin() const { return _views.begin(); }
    ViewVector::const_iterator end() const { return _views.end(); }

private:
    std::vector<std::vector<char>> _buckets;
    size_t _remainingSize = BUCKET_SIZE;
    ViewVector _views;
};

