#pragma once

#include <stdint.h>
#include <optional>
#include <utility>
#include <vector>

#include "Column.h"

#include "Bitmask.h"
#include "BioAssert.h"
#include "DebugDump.h"
#include "NameOf.h"

namespace db {

template <typename T>
class SplitColumnVector : public Column {
public:
    using BitMaskType = uint64_t;
    using ValidityMask = DynamicLargeBitMask<BitMaskType>;

    static constexpr ContainerKind::Code BaseKind = ContainerKind::code<SplitColumnVector<T>>();

    SplitColumnVector()
        : Column(_staticKind)
    {
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

    const T& getValue(size_t row) const {
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
        const bool maskExists = hasMask();
        if (maskExists) {
            _mask.reserve(_data.size() + 1);
        }

        _data.push_back(value);
        if (maskExists) {
            _mask.push_back(true);
        }
    }

    void push_back(T&& value) {
        const bool maskExists = hasMask();
        if (maskExists) {
            _mask.reserve(_data.size() + 1);
        }

        _data.push_back(std::move(value));
        if (maskExists) {
            _mask.push_back(true);
        }
    }

    void pushNull() {
        materialiseMask();
        _mask.reserve(_data.size() + 1);

        _data.emplace_back();
        _mask.push_back(false);
        ++_nullCount;
    }

    void appendRows(const SplitColumnVector& source, size_t startLine, size_t rowCount) {
        bioassert(&source != this, "SplitColumnVector::appendRows: the source is the destination");
        bioassert(startLine + rowCount <= source.size(),
                  "SplitColumnVector::appendRows: the range is past the end of the source");

        const bool sourceHasNulls = source._nullCount != 0;
        const size_t rangeNullCount = sourceHasNulls ? rowCount - source._mask.countSet(startLine, rowCount) : 0;
        const bool needsMask = rangeNullCount != 0 || hasMask();
        const size_t destinationSize = _data.size();
        if (needsMask) {
            materialiseMask();
            _mask.reserve(destinationSize + rowCount);
        }

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

        if (!hasMask()) {
            _data.resize(size);
            return;
        }

        const size_t removedCount = oldSize - size;
        const size_t removedNullCount = removedCount - _mask.countSet(size, removedCount);

        _data.resize(size);
        _mask.resize(size);
        _nullCount -= removedNullCount;
    }

    size_t getNullCount() const { return _nullCount; }
    bool hasMask() const { return !_mask.empty(); }

    std::vector<T>& getValues() { return _data; }
    const std::vector<T>& getValues() const { return _data; }

    ValidityMask* getValidity() { return hasMask() ? &_mask : nullptr; }
    const ValidityMask* getValidity() const { return hasMask() ? &_mask : nullptr; }

    void assign(const Column* other) override {
        bioassert(other->getKind() == _staticKind,
                  "SplitColumnVector::assign: other is not a SplitColumnVector of the same type");
        const SplitColumnVector<T>* otherColumn = static_cast<const SplitColumnVector<T>*>(other);

        std::vector<T> data = otherColumn->_data;
        ValidityMask mask = otherColumn->_mask;

        _data.swap(data);
        _mask.swap(mask);
        _nullCount = otherColumn->_nullCount;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        bioassert(other->getKind() == _staticKind,
                  "SplitColumnVector::assignFromLine: other is not a SplitColumnVector of the same type");
        const SplitColumnVector<T>* otherColumn = static_cast<const SplitColumnVector<T>*>(other);

        bioassert(otherColumn != this, "SplitColumnVector::assignFromLine: the source is the destination");

        const typename std::vector<T>::const_iterator otherStart = otherColumn->_data.cbegin() + startLine;
        std::vector<T> data(otherStart, otherStart + rowCount);
        ValidityMask mask {0};
        size_t nullCount = 0;

        if (otherColumn->_nullCount != 0) {
            mask.appendRange(otherColumn->_mask, startLine, rowCount);
            nullCount = rowCount - mask.countSet();
            if (nullCount == 0) {
                mask.resize(0);
            }
        }

        _data.swap(data);
        _mask.swap(mask);
        _nullCount = nullCount;
    }

    void dump(std::ostream& out) const override {
        const std::string header = fmt::format("SplitVec, sz={}, nulls={}", _data.size(), _nullCount);
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

    ContainerKind::Code getContainerKind() const override { return ContainerKind::code<SplitColumnVector<T>>(); }
    InternalKind::Code getInternalKind() const override { return InternalKind::code<T>(); }

    std::string_view getTypeName() const override { return NameOf<SplitColumnVector<T>>::get(); }

private:
    std::vector<T> _data;
    ValidityMask _mask {0};
    size_t _nullCount {0};

    static constexpr auto _staticKind = ColumnKind::code<SplitColumnVector<T>>();

    void markValid(size_t row) {
        if (_nullCount != 0 && !_mask.test(row)) {
            _mask.set(row);
            --_nullCount;
        }
    }

    void materialiseMask() {
        if (hasMask()) {
            return;
        }

        _mask.resize(_data.size());
        _mask.fill(0, _data.size(), true);
    }
};

template <class T>
struct ColumnSupportFor<SplitColumnVector<T>> : std::true_type {};

}
