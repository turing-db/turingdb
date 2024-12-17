#pragma once

#include <cstring>
#include <vector>
#include <string_view>

#include "BioAssert.h"

class StringContainer {
public:
    static constexpr size_t BUCKET_SIZE = 13;
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
                auto& chars = _buckets.emplace_back();
                chars.resize(content.size());
                std::memcpy(chars.data(), content.data(), content.size());
                _views.push_back({chars.data(), content.size()});

                _remainingSize = 0;
                return;
            } else {
                _remainingSize = BUCKET_SIZE;
                auto& chars = _buckets.emplace_back();
                chars.resize(BUCKET_SIZE);
            }
        }

        auto& chars = _buckets.back();
        const size_t offset = chars.size() - _remainingSize;
        _remainingSize -= content.size();

        std::memcpy(chars.data() + offset, content.data(), content.size());
        _views.push_back({chars.data() + offset, content.size()});
    }


    const std::string_view& getView(size_t index) const {
        msgbioassert(index < _views.size(), "String index invalid");
        return _views[index];
    }

    size_t size() const { return _views.size(); }

    const ViewVector& get() const { return _views; }

    size_t byteCount() const {
        size_t count = 0;
        for (const auto& bucket : _buckets) {
            count += bucket.size();
        }
        return count;
    }

    ViewVector::const_iterator begin() const { return _views.begin(); }
    ViewVector::const_iterator end() const { return _views.end(); }

private:
    std::vector<std::vector<char>> _buckets;
    size_t _remainingSize = BUCKET_SIZE;
    ViewVector _views;
};

