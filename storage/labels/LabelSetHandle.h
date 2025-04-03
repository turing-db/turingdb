#pragma once

#include <span>

#include "LabelSet.h"

namespace db {
template <LabelSetClass LabelSetT>
class TemplateLabelSetHandle;

}

namespace std {
template <db::LabelSetClass LabelSetT>
string to_string(const LabelSetT& set);
}

namespace db {

template <LabelSetClass LabelSetT>
class TemplateLabelSetHandle {
public:
    using BaseType = LabelSetT;
    using IntegerType = typename LabelSetT::IntegerType;
    static constexpr size_t IntegerSize = LabelSetT::IntegerSize;
    static constexpr size_t IntegerCount = LabelSetT::IntegerCount;
    static constexpr size_t BitCount = LabelSetT::BitCount;

    TemplateLabelSetHandle() = default;

    TemplateLabelSetHandle(LabelSetID id, const LabelSetT& base)
        : _id(id),
          _integers {base.integers()} {
    }

    explicit TemplateLabelSetHandle(const LabelSetT& base)
        : _integers {base.integers()} {
    }

    TemplateLabelSetHandle(const TemplateLabelSetHandle&) = default;
    TemplateLabelSetHandle(TemplateLabelSetHandle&& other) noexcept = default;

    TemplateLabelSetHandle& operator=(const TemplateLabelSetHandle&) = default;
    TemplateLabelSetHandle& operator=(TemplateLabelSetHandle&& other) noexcept = default;


    ~TemplateLabelSetHandle() = default;

    LabelSetID getID() const { return _id; };

    bool operator==(const LabelSetT& other) const {
        const std::span otherIntegers = other.integers();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (_integers[i] != otherIntegers[i]) {
                return false;
            }
        }
        return true;
    };

    bool operator==(const TemplateLabelSetHandle& other) const {
        const std::span otherIntegers = other.integers();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (_integers[i] != otherIntegers[i]) {
                return false;
            }
        }
        return true;
    };

    bool operator!=(const LabelSetT& other) const {
        const std::span otherIntegers = other.integers();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (_integers[i] != otherIntegers[i]) {
                return true;
            }
        }
        return false;
    };

    bool operator!=(const TemplateLabelSetHandle& other) const {
        const std::span otherIntegers = other.integers();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (_integers[i] != otherIntegers[i]) {
                return true;
            }
        }
        return false;
    };

    bool operator>(const TemplateLabelSetHandle& other) const {
        const std::span otherIntegers = other.integers();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (_integers[i] != otherIntegers[i]) {
                return _integers[i] > otherIntegers[i];
            }
        }
        return false;
    }

    bool operator>(const LabelSetT& other) const {
        const std::span otherIntegers = other.integers();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (_integers[i] != otherIntegers[i]) {
                return _integers[i] > otherIntegers[i];
            }
        }
        return false;
    }

    bool operator<(const TemplateLabelSetHandle& other) const {
        const std::span otherIntegers = other.integers();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (_integers[i] != otherIntegers[i]) {
                return _integers[i] < otherIntegers[i];
            }
        }
        return false;
    }

    bool operator<(const LabelSetT& other) const {
        const std::span otherIntegers = other.integers();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (_integers[i] != otherIntegers[i]) {
                return _integers[i] < otherIntegers[i];
            }
        }
        return false;
    }

    bool operator<=(const TemplateLabelSetHandle& other) const {
        return !(*this > other);
    }

    bool operator<=(const LabelSetT& other) const {
        return !(*this > other);
    }

    bool operator>=(const TemplateLabelSetHandle& other) const {
        return !(*this < other);
    }

    bool operator>=(const LabelSetT& other) const {
        return !(*this < other);
    }

    bool hasLabel(LabelID id) const {
        const auto [integerOffset, compare] = computeBitShift(id);
        return (_integers[integerOffset] & compare) == compare;
    }

    bool hasAtLeastLabels(const TemplateLabelSetHandle& query) const {
        for (size_t i = 0; i < IntegerCount; i++) {
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

    static std::pair<IntegerType, IntegerType> computeBitShift(LabelID labelID) {
        const size_t integerOffset = labelID.getValue() / IntegerSize;
        const IntegerType bitPosition = labelID.getValue() % IntegerSize;
        const IntegerType bitShift = ((IntegerType)1 << bitPosition);
        return {integerOffset, bitShift};
    }

    const IntegerType* data() const {
        return _integers.data();
    }

    IntegerType* data() {
        return _integers.data();
    }

    std::span<const IntegerType, IntegerCount> integers() const {
        return _integers;
    }

    bool empty() const { return size() == 0; }
    bool isValid() const { return _integers.size() == IntegerCount; }
    bool isStored() const { return _id.isValid(); }

    struct Hash {
        using is_transparent = void; // Enable overload resolution during hashing

        std::size_t operator()(const LabelSetT& labelset) const {
            return LabelSetT::hashIntegers(labelset.integers());
        }

        std::size_t operator()(const TemplateLabelSetHandle& labelset) const {
            return LabelSetT::hashIntegers(labelset.integers());
        }
    };

    struct Equal {
        using is_transparent = void; // Enable overload resolution during comparison

        bool operator()(const LabelSetT& lhs, const LabelSetT& rhs) const {
            return lhs == rhs;
        }

        bool operator()(const LabelSetT& lhs, const TemplateLabelSetHandle& rhs) const {
            return lhs == rhs;
        }

        bool operator()(const TemplateLabelSetHandle& lhs, const LabelSetT& rhs) const {
            return lhs == rhs;
        }

        bool operator()(const TemplateLabelSetHandle& lhs, const TemplateLabelSetHandle& rhs) const {
            return lhs == rhs;
        }
    };

private:
    friend std::hash<TemplateLabelSetHandle>;
    LabelSetID _id;

    static inline constexpr std::array<IntegerType, IntegerCount> _emptyArray {};
    std::span<const IntegerType, IntegerCount> _integers = _emptyArray;
};

using LabelSetHandle = TemplateLabelSetHandle<LabelSet>;

}

namespace std {

template <db::LabelSetClass LabelSetT>
struct hash<db::TemplateLabelSetHandle<LabelSetT>> {
    size_t operator()(const db::TemplateLabelSetHandle<LabelSetT>& set) const {
        return LabelSetT::hashIntegers(set.integers());
    }
};

}
