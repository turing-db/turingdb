#pragma once

#include <span>

#include "LabelSet.h"

namespace db {
template <std::integral TType, size_t TCount>
class TemplateLabelSetHandle;

}

namespace std {
template <std::integral TType, size_t TCount>
string to_string(const db::TemplateLabelSetHandle<TType, TCount>& set);
}

namespace db {

template <std::integral TType, size_t TCount>
class TemplateLabelSetHandle {
public:
    using BaseType = TemplateLabelSet<TType, TCount>;
    using IntegerType = typename BaseType::IntegerType;
    static constexpr size_t IntegerSize = BaseType::IntegerSize;
    static constexpr size_t IntegerCount = BaseType::IntegerCount;
    static constexpr size_t BitCount = BaseType::BitCount;

    TemplateLabelSetHandle() = default;

    TemplateLabelSetHandle(LabelSetID id, const BaseType& base)
        : _id(id),
          _labelset {&base}
    {
    }

    explicit TemplateLabelSetHandle(const BaseType& base)
        : _labelset {&base}
    {
    }

    TemplateLabelSetHandle(const TemplateLabelSetHandle&) = default;
    TemplateLabelSetHandle(TemplateLabelSetHandle&& other) noexcept = default;

    TemplateLabelSetHandle& operator=(const TemplateLabelSetHandle&) = default;
    TemplateLabelSetHandle& operator=(TemplateLabelSetHandle&& other) noexcept = default;


    ~TemplateLabelSetHandle() = default;

    LabelSetID getID() const { return _id; };

    bool operator==(const BaseType& other) const {
        const std::span otherIntegers = other.integers();
        const auto* integers = _labelset->data();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (integers[i] != otherIntegers[i]) {
                return false;
            }
        }
        return true;
    };

    bool operator==(const TemplateLabelSetHandle& other) const {
        if (!other._labelset || !_labelset) {
            return false;
        }

        const std::span otherIntegers = other.integers();
        const auto* integers = _labelset->data();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (integers[i] != otherIntegers[i]) {
                return false;
            }
        }
        return true;
    };

    bool operator!=(const BaseType& other) const {
        const std::span otherIntegers = other.integers();
        const auto* integers = _labelset->data();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (integers[i] != otherIntegers[i]) {
                return true;
            }
        }
        return false;
    };

    bool operator!=(const TemplateLabelSetHandle& other) const {
        if (!other._labelset || !_labelset) {
            return true;
        }

        const std::span otherIntegers = other.integers();
        const auto* integers = _labelset->data();

        for (size_t i = 0; i < IntegerCount; i++) {
            if (integers[i] != otherIntegers[i]) {
                return true;
            }
        }
        return false;
    };

    bool operator>(const TemplateLabelSetHandle& other) const {
        if (!other._labelset || !_labelset) {
            return true;
        }

        const std::span otherIntegers = other.integers();
        const auto* integers = _labelset->data();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (integers[i] != otherIntegers[i]) {
                return integers[i] > otherIntegers[i];
            }
        }
        return false;
    }

    bool operator>(const BaseType& other) const {
        const std::span otherIntegers = other.integers();
        const auto* integers = _labelset->data();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (integers[i] != otherIntegers[i]) {
                return integers[i] > otherIntegers[i];
            }
        }
        return false;
    }

    bool operator<(const TemplateLabelSetHandle& other) const {
        if (!other._labelset || !_labelset) {
            return false;
        }

        const std::span otherIntegers = other.integers();
        const auto* integers = _labelset->data();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (integers[i] != otherIntegers[i]) {
                return integers[i] < otherIntegers[i];
            }
        }
        return false;
    }

    bool operator<(const BaseType& other) const {
        const std::span otherIntegers = other.integers();
        const auto* integers = _labelset->data();
        for (size_t i = 0; i < IntegerCount; i++) {
            if (integers[i] != otherIntegers[i]) {
                return integers[i] < otherIntegers[i];
            }
        }
        return false;
    }

    bool operator<=(const TemplateLabelSetHandle& other) const {
        return !(*this > other);
    }

    bool operator<=(const BaseType& other) const {
        return !(*this > other);
    }

    bool operator>=(const TemplateLabelSetHandle& other) const {
        return !(*this < other);
    }

    bool operator>=(const BaseType& other) const {
        return !(*this < other);
    }

    bool hasLabel(LabelID id) const {
        const auto* integers = _labelset->data();
        const auto [integerOffset, compare] = computeBitShift(id);
        return (integers[integerOffset] & compare) == compare;
    }

    bool hasAtLeastLabels(const TemplateLabelSetHandle& query) const {
        const auto* integers = _labelset->data();
        const auto* queryIntegers = query.data();
        for (size_t i = 0; i < IntegerCount; i++) {
            if ((integers[i] & queryIntegers[i]) != queryIntegers[i]) {
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
        for (auto i : _labelset->integers()) {
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
        return _labelset->data();
    }

    IntegerType* data() {
        return _labelset->data();
    }

    std::span<const IntegerType, IntegerCount> integers() const {
        return _labelset->integers();
    }

    bool empty() const { return size() == 0; }
    bool isValid() const { return _labelset != nullptr; }
    bool isStored() const { return _id.isValid(); }

    const BaseType& get() const {
        return *_labelset;
    }

    struct Hash {
        using is_transparent = void; // Enable overload resolution during hashing

        std::size_t operator()(const BaseType& labelset) const {
            return BaseType::hashIntegers(labelset.integers());
        }

        std::size_t operator()(const TemplateLabelSetHandle& labelset) const {
            return BaseType::hashIntegers(labelset.integers());
        }
    };

    struct Equal {
        using is_transparent = void; // Enable overload resolution during comparison

        bool operator()(const BaseType& lhs, const BaseType& rhs) const {
            return lhs == rhs;
        }

        bool operator()(const BaseType& lhs, const TemplateLabelSetHandle& rhs) const {
            return lhs == rhs;
        }

        bool operator()(const TemplateLabelSetHandle& lhs, const BaseType& rhs) const {
            return lhs == rhs;
        }

        bool operator()(const TemplateLabelSetHandle& lhs, const TemplateLabelSetHandle& rhs) const {
            return lhs == rhs;
        }
    };

private:
    friend std::hash<TemplateLabelSetHandle>;

    LabelSetID _id;
    const BaseType* _labelset {nullptr};
};

using LabelSetHandle = TemplateLabelSetHandle<LabelSet::IntegerType, LabelSet::IntegerCount>;

}

namespace std {

template <std::integral TType, size_t TCount>
struct hash<db::TemplateLabelSetHandle<TType, TCount>> {
    size_t operator()(const db::TemplateLabelSetHandle<TType, TCount>& set) const {
        return db::TemplateLabelSet<TType, TCount>::hashIntegers(set.integers());
    }
};

}
