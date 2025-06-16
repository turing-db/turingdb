#pragma once

#include <bit>
#include <span>

#include "ID.h"

namespace db {
template <std::integral TType, size_t TCount>
class TemplateLabelSet;

template <std::integral TType, size_t TCount>
class TemplateLabelSetHandle;
}

namespace std {
template <std::integral TType, size_t TCount>
string to_string(const db::TemplateLabelSet<TType, TCount>& set);
}

namespace db {

template <std::integral TType, size_t TCount>
class TemplateLabelSet {
public:
    using IntegerType = TType;
    using Handle = TemplateLabelSetHandle<TType, TCount>;

    static constexpr size_t IntegerSize = sizeof(TType) * 8;
    static constexpr size_t IntegerCount = TCount;
    static constexpr size_t BitCount = IntegerSize * TCount;

    TemplateLabelSet() = default;
    TemplateLabelSet(const TemplateLabelSet&) = default;
    TemplateLabelSet(TemplateLabelSet&& other) noexcept
        : _integers(other._integers)
    {
    }

    TemplateLabelSet& operator=(const TemplateLabelSet&) = default;
    TemplateLabelSet& operator=(TemplateLabelSet&& other) noexcept {
        _integers = other._integers;
        return *this;
    }

    TemplateLabelSet& operator=(std::initializer_list<LabelID> labels) {
        for (auto& integer : _integers) {
            integer = 0;
        }

        for (const auto& id : labels) {
            const auto [integerOffset, bitShift] = computeBitShift(id);
            _integers[integerOffset] |= bitShift;
        }
    }

    static TemplateLabelSet fromList(std::initializer_list<LabelID> labels) {
        TemplateLabelSet labelset;
        for (const auto& id : labels) {
            const auto [integerOffset, bitShift] = computeBitShift(id);
            labelset._integers[integerOffset] |= bitShift;
        }
        return labelset;
    }

    static TemplateLabelSet fromIntegers(std::span<const IntegerType, IntegerCount> integers) {
        TemplateLabelSet labelset;
        for (size_t i = 0; i < IntegerCount; i++) {
            labelset._integers[i] = integers[i];
        }
        return labelset;
    }

    ~TemplateLabelSet() = default;

    void set(LabelID id) {
        const auto [integerOffset, bitShift] = computeBitShift(id);
        _integers[integerOffset] |= bitShift;
    }

    void unset(LabelID id) {
        const auto [integerOffset, bitShift] = computeBitShift(id);
        _integers[integerOffset] &= ~bitShift;
    }

    bool operator==(const TemplateLabelSet& other) const {
        return _integers == other._integers;
    };

    bool operator!=(const TemplateLabelSet& other) const {
        return _integers != other._integers;
    };

    bool operator>(const TemplateLabelSet& other) const {
        for (size_t i = 0; i < TCount; i++) {
            if (_integers[i] != other._integers[i]) {
                return _integers[i] > other._integers[i];
            }
        }
        return false;
    }

    bool operator<(const TemplateLabelSet& other) const {
        for (size_t i = 0; i < TCount; i++) {
            if (_integers[i] != other._integers[i]) {
                return _integers[i] < other._integers[i];
            }
        }
        return false;
    }

    bool operator<=(const TemplateLabelSet& other) const {
        return !(*this > other);
    }

    bool operator>=(const TemplateLabelSet& other) const {
        return !(*this < other);
    }

    bool hasLabel(LabelID id) const {
        const auto [integerOffset, compare] = computeBitShift(id);
        return (_integers[integerOffset] & compare) == compare;
    }

    bool hasAtLeastLabels(const TemplateLabelSet& query) const {
        for (size_t i = 0; i < TCount; i++) {
            if ((_integers[i] & query._integers[i]) != query._integers[i]) {
                return false;
            }
        }
        return true;
    }

    void decompose(std::vector<LabelID>& labels) const {
        for (LabelID id = 0; id < IntegerSize * IntegerCount; id++) {
            if (hasLabel(id)) {
                labels.push_back(id);
            }
        }
    }

    size_t size() const {
        size_t count = 0;
        for (auto i : _integers) {
            count += std::popcount(i);
        }
        return count;
    }

    static std::pair<TType, TType> computeBitShift(LabelID labelID) {
        const size_t integerOffset = labelID.getValue() / IntegerSize;
        const TType bitPosition = labelID.getValue() % IntegerSize;
        const TType bitShift = ((TType)1 << bitPosition);
        return {integerOffset, bitShift};
    }

    const TType* data() const {
        return _integers.data();
    }

    TType* data() {
        return _integers.data();
    }

    std::span<const TType, IntegerCount> integers() const {
        return _integers;
    }

    bool empty() const { return size() == 0; }

    static size_t hashIntegers(std::span<const TType, IntegerCount> integers) {
        size_t seed = TCount;
        for (auto x : integers) {
            seed ^= std::hash<TType> {}(x);
        }
        return seed;
    }

    Handle handle() const {
        return Handle {*this};
    }
private:
    friend std::hash<TemplateLabelSet>;
    std::array<TType, TCount> _integers {};
};

using LabelSet = TemplateLabelSet<uint64_t, 4>;

}

namespace std {

template <std::integral TType, size_t TCount>
struct hash<db::TemplateLabelSet<TType, TCount>> {
    size_t operator()(const db::TemplateLabelSet<TType, TCount>& set) const {
        return db::TemplateLabelSet<TType, TCount>::hashIntegers(set.integers());
    }
};

}
