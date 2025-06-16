#pragma once

#include <cstring>
#include <span>
#include <vector>
#include <string_view>

#include "BioAssert.h"
#include "StringBucket.h"

namespace db {

class StringContainer {
public:
    using ViewVector = std::vector<std::string_view>;
    using BucketVector = std::vector<StringBucket>;

    StringContainer()
        : _buckets(1) 
    {
    }

    ~StringContainer() = default;
    StringContainer(StringContainer&& other) noexcept = default;
    StringContainer& operator=(StringContainer&& other) noexcept = default;
    StringContainer(const StringContainer& other) = delete;
    StringContainer& operator=(const StringContainer& other) = delete;

    bool alloc(std::string_view content) {
        if (StringBucket::BUCKET_SIZE < content.size()) {
            return false;
        }

        StringBucket* bucket = &_buckets.back();
        if (bucket->availSpace() < content.size()) {
            bucket = &_buckets.emplace_back();
        }

        _views.push_back(bucket->alloc(content));

        return true;
    }

    void clear() {
        _views.clear();
        _buckets.clear();
    }

    void addBucket(StringBucket&& bucket) {
        _buckets.push_back(std::move(bucket));
        auto& b = _buckets.back();

        const std::span limits = b.limits();
        const size_t prevCount = _views.size();
        const size_t newCount = prevCount + limits.size();

        _views.resize(newCount);

        // Building views from bucket
        for (size_t i = 0; i < limits.size(); i++) {
            _views[i + prevCount] = {
                b.data() + limits[i]._offset,
                limits[i]._count,
            };
        }
    }

    bool addBucket(std::span<const char> chars,
                   std::span<const StringBucket::StringLimits> limits) {
        auto bucket = StringBucket::create(chars, limits);

        if (!bucket.has_value()) {
            return false;
        }

        addBucket(std::move(bucket.value()));
        return true;
    }

    bool addBucket(std::vector<char>&& chars,
                   std::vector<StringBucket::StringLimits>&& limits) {
        auto bucket = StringBucket::create(std::move(chars), std::move(limits));

        if (!bucket.has_value()) {
            return false;
        }

        addBucket(std::move(bucket.value()));
        return true;
    }

    const std::string_view& getView(size_t index) const {
        msgbioassert(index < _views.size(), "String index invalid");
        return _views[index];
    }

    size_t size() const { return _views.size(); }

    const ViewVector& get() const { return _views; }
    size_t bucketCount() const { return _buckets.size(); }
    size_t countInBucket(size_t bucket) const { return _buckets[bucket].strCount(); }

    const StringBucket& bucket(size_t i) const {
        return _buckets[i];
    }

    const BucketVector& buckets() const {
        return _buckets;
    }

    ViewVector::const_iterator begin() const { return _views.begin(); }
    ViewVector::const_iterator end() const { return _views.end(); }

private:
    BucketVector _buckets;
    ViewVector _views;
};

}
