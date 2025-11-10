#pragma once

#include <vector>

#include "Column.h"

#include "DebugDump.h"
#include "BioAssert.h"

namespace db {

class ColumnMask : public Column {
public:
    struct Bool_t {
        Bool_t() = default;
        Bool_t(bool v)
            : _value(v)
        {
        }

        Bool_t& operator=(bool v) {
            _value = v;
            return *this;
        }
        operator bool() const { return _value; }

        bool _value {false};
    };

    using ValueType = Bool_t;
    using Iterator = std::vector<Bool_t>::iterator;
    using ConstIterator = std::vector<Bool_t>::const_iterator;
    using ReverseIterator = std::vector<Bool_t>::reverse_iterator;
    using ConstReverseIterator = std::vector<Bool_t>::const_reverse_iterator;
    static constexpr ColumnKind::ColumnKindCode BaseKind = (ColumnKind::ColumnKindCode)ColumnKind::BaseColumnKind::MASK;

    ColumnMask(const ColumnMask&) = default;
    ColumnMask(ColumnMask&&) noexcept = default;

    ColumnMask()
        : Column(_staticKind)
    {
    }

    ColumnMask(std::initializer_list<Bool_t>&& v)
        : Column(_staticKind),
          _data(std::forward<std::initializer_list<Bool_t>>(v))
    {
    }

    explicit ColumnMask(const std::vector<Bool_t>& vec)
        : Column(_staticKind),
          _data(vec)
    {
    }

    explicit ColumnMask(std::vector<Bool_t>&& vec) noexcept
        : Column(_staticKind),
          _data(std::move(vec))
    {
    }

    explicit ColumnMask(size_t size)
        : Column(_staticKind),
          _data(size)
    {
    }

    ~ColumnMask() override = default;

    ColumnMask& operator=(const ColumnMask&) = default;
    ColumnMask& operator=(ColumnMask&&) noexcept = default;

    bool empty() const { return _data.empty(); }
    size_t size() const override { return _data.size(); }
    const Bool_t* data() const { return _data.data(); }
    Bool_t* data() { return _data.data(); }

    const bool& operator[](size_t i) const { return _data[i]._value; }
    bool& operator[](size_t i) { return _data[i]._value; }
    const bool& at(size_t i) const { return _data.at(i)._value; }
    bool& at(size_t i) { return _data.at(i)._value; }

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
        const ColumnMask* otherCol = dynamic_cast<const ColumnMask*>(other);
        msgbioassert(otherCol, "ColumnMask::assign: other is not a ColumnMask of the same type");
        _data = otherCol->_data;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        const ColumnMask* otherCol = dynamic_cast<const ColumnMask*>(other);
        msgbioassert(otherCol, "ColumnMask::assignFromLine: other is not a ColumnMask of the same type");

        _data.clear();
        const auto otherStart = otherCol->_data.cbegin() + startLine;
        _data.assign(otherStart, otherStart + rowCount);
    }

    void clear() { _data.clear(); }

    void resize(size_t size) { _data.resize(size); }

    void push_back(bool v) { _data.push_back(Bool_t{v}); }
    void reserve(size_t capacity) { _data.reserve(capacity); }

    size_t capacity() const { return _data.capacity(); }

    template <typename... Args>
    auto emplace_back(Args&&... args) {
        return _data.emplace_back(std::forward(args)...);
    }

    void dump(std::ostream& out) const override {
        DebugDump::dumpString(out, "ColumnMask of size="+std::to_string(_data.size()));
        for (const auto& elem : _data) {
            DebugDump::dump(out, elem);
        }
    }

    const bool& back() const { return _data.back()._value; }
    bool& back() { return _data.back()._value; }
    const bool& front() const { return _data.front()._value; }
    bool& front() { return _data.front()._value; }

    std::vector<Bool_t>& getRaw() { return _data; }
    const std::vector<Bool_t>& getRaw() const { return _data; }


    static consteval auto staticKind() { return _staticKind; }

private:
    std::vector<Bool_t> _data;

    static constexpr auto _staticKind = ColumnKind::getColumnKind<ColumnMask>();
};

}
