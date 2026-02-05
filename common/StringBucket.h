#pragma once

#include <stdint.h>
#include <string.h>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "BioAssert.h"

class StringBucket {
public:
    struct StringLimits {
        uint32_t _offset {0};
        uint32_t _count {0};
    };

    static constexpr uint32_t BUCKET_SIZE = 256ul * 1024;
    static_assert(BUCKET_SIZE <= std::numeric_limits<uint32_t>::max());

    StringBucket() 
        : _bucket(BUCKET_SIZE)
    {
    }

    ~StringBucket() = default;

    StringBucket(const StringBucket&) = delete;
    StringBucket(StringBucket&&) noexcept = default;
    StringBucket& operator=(const StringBucket&) = delete;
    StringBucket& operator=(StringBucket&&) noexcept = default;

    uint32_t strCount() const { return _limits.size(); }
    uint32_t charCount() const { return _charCount; }
    uint32_t availSpace() const { return BUCKET_SIZE - _charCount; }
    std::span<const char> span() const { return _bucket; }
    std::span<const StringLimits> limits() const { return _limits; }
    const char* data() const { return _bucket.data(); }

    std::string_view alloc(std::string_view content) {
        bioassert(availSpace() >= content.size(), "String does not fit in bucket");
        const uint32_t offset = _charCount;
        const uint32_t count = content.size();

        char* dst = _bucket.data() + offset;
        std::memcpy(dst, content.data(), count);

        _limits.push_back({offset, count});
        _charCount += count;

        return std::string_view {dst, count};
    }

    [[nodiscard]] static std::optional<StringBucket> create(std::vector<char>&& chars,
                                                            std::vector<StringLimits>&& limits) {
        if (chars.size() != BUCKET_SIZE) {
            return std::nullopt;
        }

        StringBucket bucket;
        bucket._bucket = std::move(chars);
        bucket._limits = std::move(limits);

        if (!bucket._limits.empty()) {
            const auto& lastLim = bucket._limits.back();
            bucket._charCount = lastLim._offset + lastLim._count;
        }

        return bucket;
    }

    [[nodiscard]] static std::optional<StringBucket> create(std::span<const char> chars,
                                                            std::span<const StringLimits> limits) {
        std::vector<char> charVector(BUCKET_SIZE);
        std::vector<StringLimits> limitVector(limits.size());

        std::memcpy(charVector.data(), chars.data(), chars.size());
        std::memcpy(limitVector.data(), limits.data(), limits.size() * sizeof(StringLimits));

        return create(std::move(charVector), std::move(limitVector));
    }

private:
    std::vector<char> _bucket;
    std::vector<StringLimits> _limits;
    uint32_t _charCount {0};
};
