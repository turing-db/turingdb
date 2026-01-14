#pragma once

#include <vector>

#include "Column.h"
#include "DebugDump.h"

#include "BioAssert.h"

namespace db {

template <typename T>
class ColumnVector : public Column {
public:
    using ValueType = T;
    using Iterator = std::vector<T>::iterator;
    using ConstIterator = std::vector<T>::const_iterator;
    using ReverseIterator = std::vector<T>::reverse_iterator;
    using ConstReverseIterator = std::vector<T>::const_reverse_iterator;

    static constexpr ContainerKind::Code BaseKind = ContainerKind::code<ColumnVector<T>>();

    ColumnVector(const ColumnVector&) = default;
    ColumnVector(ColumnVector&&) noexcept = default;

    ColumnVector()
        : Column(_staticKind)
    {
    }

    ColumnVector(std::initializer_list<T> v)
        : Column(_staticKind),
        _data(v)
    {
    }

    explicit ColumnVector(const std::vector<T>& vec)
        : Column(_staticKind),
        _data(vec)
    {
    }

    explicit ColumnVector(std::vector<T>&& vec) noexcept
        : Column(_staticKind),
        _data(std::move(vec))
    {
    }

    explicit ColumnVector(size_t size)
        : Column(_staticKind),
        _data(size)
    {
    }

    explicit ColumnVector(size_t size, const T& value)
        : Column(_staticKind),
        _data(size, value)
    {
    }

    ~ColumnVector() override = default;

    ColumnVector& operator=(const ColumnVector&) = default;
    ColumnVector& operator=(ColumnVector&&) noexcept = default;

    bool empty() const { return _data.empty(); }
    size_t size() const override { return _data.size(); }

    T* data() { return _data.data(); }
    const T* data() const { return _data.data(); }

    const T& operator[](size_t i) const { return _data[i]; }
    T& operator[](size_t i) { return _data[i]; }
    const T& at(size_t i) const { return _data.at(i); }
    T& at(size_t i) { return _data.at(i); }

    void set(size_t i, T&& v) { _data[i] = std::move(v); }
    void set(size_t i, const T& v) { _data[i] = v; }
    T getCopy(size_t i) const { return _data[i]; }

    Iterator begin() { return _data.begin(); }
    ConstIterator begin() const { return _data.begin(); }
    ConstIterator cbegin() const { return _data.cbegin(); }
    ReverseIterator rbegin() { return _data.rbegin(); }
    ConstReverseIterator rbegin() const { return _data.rbegin(); }
    ConstReverseIterator crbegin() const { return _data.crbegin(); }

    Iterator end() { return _data.end(); }
    ConstIterator end() const { return _data.end(); }
    ConstIterator cend() const { return _data.cend(); }
    ReverseIterator rend() { return _data.rend(); }
    ConstReverseIterator rend() const { return _data.rend(); }
    ConstReverseIterator crend() const { return _data.crend(); }

    void assign(const Column* other) override {
        const ColumnVector<T>* otherCol = dynamic_cast<const ColumnVector<T>*>(other);
        bioassert(otherCol, "ColumnVector::assign: other is not a ColumnVector of the same type");
        _data = otherCol->_data;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        const ColumnVector<T>* otherCol = dynamic_cast<const ColumnVector<T>*>(other);
        bioassert(otherCol, "ColumnVector::assignFromLine: other is not a ColumnVector of the same type");

        _data.clear();
        const auto otherStart = otherCol->_data.cbegin() + startLine;
        _data.assign(otherStart, otherStart + rowCount);
    }

    void clear() { _data.clear(); }

    void resize(size_t size) { _data.resize(size); }

    void push_back(const T& v) { _data.push_back(v); }
    void reserve(size_t capacity) { _data.reserve(capacity); }

    size_t capacity() const { return _data.capacity(); }

    template <typename... Args>
    auto& emplace_back(Args&&... args) {
        return _data.emplace_back(std::forward<Args>(args)...);
    }

    void dump(std::ostream& out) const override {
        const std::string str = fmt::format("Vec, sz={}", _data.size());
        const std::string ptr = fmt::format("@{}", fmt::ptr(this));
        DebugDump::dumpString(out, str);
        DebugDump::dumpString(out, ptr);
        for (const auto& elem : _data) {
            DebugDump::dump(out, elem);
        }
    }

    const T& back() const { return _data.back(); }
    T& back() { return _data.back(); }
    const T& front() const { return _data.front(); }
    T& front() { return _data.front(); }

    std::vector<T>& getRaw() { return _data; }
    const std::vector<T>& getRaw() const { return _data; }

    static consteval auto staticKind() { return _staticKind; }

private:
    std::vector<T> _data;

    static constexpr auto _staticKind = ColumnKind::code<ColumnVector<T>>();
};

}
