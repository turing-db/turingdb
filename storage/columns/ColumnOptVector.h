#pragma once

#include <stdint.h>
#include <algorithm>
#include <bit>
#include <functional>
#include <initializer_list>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "Column.h"
#include "ColumnVector.h"
#include "OptionalTraits.h"

#include "Bitmask.h"
#include "BioAssert.h"
#include "DebugDump.h"
#include "NameOf.h"

namespace db {

template <typename T>
class ColumnOptVector : public Column {
public:
    using ValueType = std::optional<T>;
    using UnwrappedType = T;
    using BitMaskType = uint64_t;
    using ValidityMask = DynamicLargeBitMask<BitMaskType>;

    static_assert(!OptionalTraits::is_optional_v<T>, "A ColumnOptVector holds the unwrapped value type");

    static constexpr bool IS_NULLABLE = true;
    static constexpr ContainerKind::Code BaseKind = ContainerKind::code<ColumnOptVector<T>>();

    ColumnOptVector()
        : Column(_staticKind)
    {
    }

    ColumnOptVector(std::initializer_list<std::optional<T>> values)
        : Column(_staticKind)
    {
        _data.reserve(values.size());

        for (const std::optional<T>& value : values) {
            if (value.has_value()) {
                push_back(*value);
            } else {
                pushNull();
            }
        }
    }

    size_t size() const override { return _data.size(); }
    bool empty() const { return _data.empty(); }

    void clear() override {
        _data.clear();
        _mask.resize(0);
        _nullCount = 0;
    }

    void reserve(size_t capacity) { _data.reserve(capacity); }

    bool isValid(size_t row) const { return _nullCount == 0 || _mask.test(row); }

    std::optional<T> get(size_t row) const {
        if (isValid(row)) {
            return _data[row];
        }

        return std::nullopt;
    }

    typename std::vector<T>::const_reference getValue(size_t row) const {
        bioassert(isValid(row), "getValue on a null row");
        return _data[row];
    }

    void set(size_t row, const T& value) {
        _data[row] = value;
        markValid(row);
    }

    void set(size_t row, T&& value) {
        _data[row] = std::move(value);
        markValid(row);
    }

    void setOptional(size_t row, const std::optional<T>& value) {
        if (value.has_value()) {
            set(row, *value);
        } else {
            setNull(row);
        }
    }

    void setOptional(size_t row, std::optional<T>&& value) {
        if (value.has_value()) {
            set(row, std::move(*value));
        } else {
            setNull(row);
        }
    }

    void setNull(size_t row) {
        materialiseMask();
        _data[row] = T {};

        if (_mask.test(row)) {
            _mask.reset(row);
            ++_nullCount;
        }
    }

    void push_back(const T& value) {
        reserveMaskForAppend(1);

        _data.push_back(value);
        if (hasMask()) {
            _mask.push_back(true);
        }
    }

    void push_back(T&& value) {
        reserveMaskForAppend(1);

        _data.push_back(std::move(value));
        if (hasMask()) {
            _mask.push_back(true);
        }
    }

    void pushNull() {
        materialiseMask();
        reserveForAppend(1);

        _data.emplace_back();
        _mask.push_back(false);
        ++_nullCount;
    }

    void appendRows(const ColumnOptVector& source, size_t startLine, size_t rowCount) {
        bioassert(&source != this, "ColumnOptVector::appendRows: the source is the destination");
        bioassert(startLine + rowCount <= source.size(),
                  "ColumnOptVector::appendRows: the range is past the end of the source");

        const bool sourceHasNulls = source._nullCount != 0;
        const size_t rangeNullCount = sourceHasNulls ? rowCount - source._mask.countSet(startLine, rowCount) : 0;
        const bool needsMask = rangeNullCount != 0 || hasMask();
        const size_t destinationSize = _data.size();
        if (needsMask) {
            materialiseMask();
        }

        reserveForAppend(rowCount);

        const typename std::vector<T>::const_iterator sourceStart = source._data.cbegin() + startLine;
        _data.insert(_data.end(), sourceStart, sourceStart + rowCount);

        if (!needsMask) {
            return;
        }

        if (sourceHasNulls) {
            _mask.appendRange(source._mask, startLine, rowCount);
        } else {
            _mask.resize(destinationSize + rowCount);
            _mask.fill(destinationSize, rowCount, true);
        }

        _nullCount += rangeNullCount;
    }

    void resize(size_t size) {
        const size_t oldSize = _data.size();
        if (size > oldSize) {
            materialiseMask();
            _mask.reserve(size);

            _data.resize(size);
            _mask.resize(size);
            _nullCount += size - oldSize;
            return;
        }

        shrink(size);
    }

    void resizeValid(size_t size) {
        const size_t oldSize = _data.size();
        if (size <= oldSize) {
            shrink(size);
            return;
        }

        if (!hasMask()) {
            _data.resize(size);
            return;
        }

        _data.resize(size);
        _mask.resize(size);
        _mask.fill(oldSize, size - oldSize, true);
    }

    void assignNulls(size_t count) {
        clear();
        resize(count);
    }

    void fillNull(size_t start, size_t count) {
        bioassert(start + count <= _data.size(), "ColumnOptVector::fillNull: the range is past the end of the column");

        if (count == 0) {
            return;
        }

        materialiseMask();
        std::fill_n(_data.begin() + start, count, T {});
        _nullCount += _mask.countSet(start, count);
        _mask.fill(start, count, false);
    }

    size_t getNullCount() const { return _nullCount; }
    bool hasMask() const { return !_mask.empty(); }

    std::vector<T>& getValues() { return _data; }
    const std::vector<T>& getValues() const { return _data; }

    ValidityMask* getValidity() { return hasMask() ? &_mask : nullptr; }
    const ValidityMask* getValidity() const { return hasMask() ? &_mask : nullptr; }

    // A row is valid where both masks hold it valid, and a missing mask holds every row
    // valid. Every null row's value is reset to T {}.
    void assignValidity(const ValidityMask* first, const ValidityMask* second) {
        if (first == nullptr && second == nullptr) {
            _mask.resize(0);
            _nullCount = 0;
            return;
        }

        const size_t size = _data.size();
        _mask.resize(size);

        BitMaskType* words = _mask.data();
        const size_t wordCount = _mask.getWordCount();

        if (first != nullptr && second != nullptr) {
            bioassert(first->size() == size && second->size() == size,
                      "ColumnOptVector::assignValidity: a mask is not the size of the column");
            std::transform(first->data(), first->data() + wordCount, second->data(), words, std::bit_and<> {});
        } else {
            const ValidityMask* source = first != nullptr ? first : second;
            bioassert(source->size() == size, "ColumnOptVector::assignValidity: the mask is not the size of the column");
            if (source != &_mask) {
                std::copy_n(source->data(), wordCount, words);
            }
        }

        settleValidity();
    }

    // Calls wordOf(w) once for each word w, in order. Row r is valid where bit r % 64 of
    // wordOf(r / 64) is set
    template <typename WordOf>
    void assignValidityWords(WordOf wordOf) {
        const size_t size = _data.size();
        _mask.resize(size);

        BitMaskType* words = _mask.data();
        const size_t wordCount = _mask.getWordCount();

        for (size_t word = 0; word < wordCount; word++) {
            words[word] = wordOf(word);
        }

        const size_t tailBitCount = size % ValidityMask::BITS_PER_ELEMENT;
        if (tailBitCount != 0) {
            words[wordCount - 1] &= ValidityMask::lowBits(tailBitCount);
        }

        settleValidity();
    }

    void assign(const Column* other) override {
        bioassert(other->getKind() == _staticKind,
                  "ColumnOptVector::assign: other is not a ColumnOptVector of the same type");
        const ColumnOptVector<T>* otherColumn = static_cast<const ColumnOptVector<T>*>(other);

        _data = otherColumn->_data;
        _mask = otherColumn->_mask;
        _nullCount = otherColumn->_nullCount;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        bioassert(other->getKind() == _staticKind,
                  "ColumnOptVector::assignFromLine: other is not a ColumnOptVector of the same type");
        const ColumnOptVector<T>* otherColumn = static_cast<const ColumnOptVector<T>*>(other);

        bioassert(otherColumn != this, "ColumnOptVector::assignFromLine: the source is the destination");

        const typename std::vector<T>::const_iterator otherStart = otherColumn->_data.cbegin() + startLine;
        _data.assign(otherStart, otherStart + rowCount);
        _mask.resize(0);
        _nullCount = 0;

        if (otherColumn->_nullCount != 0) {
            _mask.appendRange(otherColumn->_mask, startLine, rowCount);
            _nullCount = rowCount - _mask.countSet();
            if (_nullCount == 0) {
                _mask.resize(0);
            }
        }
    }

    void dump(std::ostream& out) const override {
        const std::string header = fmt::format("OptVec, sz={}, nulls={}", _data.size(), _nullCount);
        const std::string address = fmt::format("@{}", fmt::ptr(this));
        DebugDump::dumpString(out, header);
        DebugDump::dumpString(out, address);

        for (size_t row = 0; row < _data.size(); ++row) {
            if (isValid(row)) {
                DebugDump::dump(out, _data[row]);
            } else {
                DebugDump::dumpNull(out);
            }
        }
    }

    static consteval auto staticKind() { return _staticKind; }

    ContainerKind::Code getContainerKind() const override { return ContainerKind::code<ColumnOptVector<T>>(); }
    InternalKind::Code getInternalKind() const override { return InternalKind::code<T>(); }

    std::string_view getTypeName() const override { return NameOf<ColumnOptVector<T>>::get(); }

private:
    std::vector<T> _data;
    ValidityMask _mask {0};
    size_t _nullCount {0};

    static constexpr auto _staticKind = ColumnKind::code<ColumnOptVector<T>>();

    void markValid(size_t row) {
        if (_nullCount != 0 && !_mask.test(row)) {
            _mask.set(row);
            --_nullCount;
        }
    }

    void shrink(size_t size) {
        if (!hasMask()) {
            _data.resize(size);
            return;
        }

        const size_t removedCount = _data.size() - size;
        const size_t removedNullCount = removedCount - _mask.countSet(size, removedCount);

        _data.resize(size);
        _mask.resize(size);
        _nullCount -= removedNullCount;
    }

    void reserveForAppend(size_t rowCount) {
        const size_t size = _data.size() + rowCount;
        const size_t valueCapacity = _data.capacity();
        if (size > valueCapacity) {
            _data.reserve(std::max(size, 2 * valueCapacity));
        }

        reserveMaskForAppend(rowCount);
    }

    void reserveMaskForAppend(size_t rowCount) {
        const size_t size = _data.size() + rowCount;
        const size_t maskCapacity = _mask.capacity();
        if (hasMask() && size > maskCapacity) {
            _mask.reserve(std::max(size, 2 * maskCapacity));
        }
    }

    void settleValidity() {
        _nullCount = _data.size() - _mask.countSet();
        if (_nullCount == 0) {
            _mask.resize(0);
            return;
        }

        resetNullValues();
    }

    void resetNullValues() {
        constexpr size_t BITS_PER_WORD = ValidityMask::BITS_PER_ELEMENT;

        const BitMaskType* words = _mask.data();
        const size_t size = _data.size();

        for (size_t start = 0; start < size; start += BITS_PER_WORD) {
            const BitMaskType inRange = ValidityMask::lowBits(std::min(BITS_PER_WORD, size - start));
            BitMaskType nullRows = ~words[start / BITS_PER_WORD] & inRange;

            while (nullRows != 0) {
                _data[start + std::countr_zero(nullRows)] = T {};
                nullRows &= nullRows - 1;
            }
        }
    }

    void materialiseMask() {
        if (!hasMask()) {
            buildAllValidMask();
        }
    }

    [[gnu::cold, gnu::noinline]] void buildAllValidMask() {
        _mask.reserve(_data.capacity());
        _mask.resize(_data.size());
        _mask.fill(0, _data.size(), true);
    }
};

template <class T>
struct ColumnSupportFor<ColumnOptVector<T>> : std::true_type {};

template <typename T>
struct ColumnForKind<ColumnOptVector<T>> {
    using type = void;
};

template <typename T>
struct ColumnForKind<ColumnOptVector<std::optional<T>>> {
    using type = ColumnOptVector<T>;
};

template <typename T>
using ColumnVectorOf = std::conditional_t<OptionalTraits::is_optional_v<T>,
                                          ColumnOptVector<OptionalTraits::unwrap_optional_t<T>>,
                                          ColumnVector<T>>;

}
