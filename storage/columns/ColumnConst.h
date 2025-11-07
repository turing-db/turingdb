#pragma once

#include "Column.h"
#include "ColumnKind.h"

#include "DebugDump.h"
#include "BioAssert.h"
#include "FatalException.h"

namespace db {

template <typename T>
class ColumnConst : public Column {
public:
    using ValueType = T;
    static constexpr ColumnKind::ColumnKindCode BaseKind = (ColumnKind::ColumnKindCode)ColumnKind::BaseColumnKind::CONST;

    ColumnConst()
        : Column(_staticKind)
    {
    }


    explicit ColumnConst(T&& value)
        : Column(_staticKind),
          _value(std::forward<T>(value))
    {
    }

    ~ColumnConst() override = default;

    ColumnConst(const ColumnConst&) = default;
    ColumnConst(ColumnConst&&) noexcept = default;

    ColumnConst& operator=(T&& value) { _value = std::forward<T>(value); return *this; }
    ColumnConst& operator=(const ColumnConst&) = default;
    ColumnConst& operator=(ColumnConst&&) noexcept = default;

    explicit operator T() const { return _value; }

    size_t size() const override { return 1; }

    void resize([[maybe_unused]] size_t newSize) override {
        throw FatalException("Attempted to resize ColumnConst.");
    }

    void assign(const Column* other) override {
        const ColumnConst<T>* otherCol = dynamic_cast<const ColumnConst<T>*>(other);
        msgbioassert(otherCol, "ColumnConst::assign: other is not a ColumnConst of the same type");
        _value = otherCol->_value;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        const ColumnConst<T>* otherCol = dynamic_cast<const ColumnConst<T>*>(other);
        msgbioassert(otherCol, "ColumnConst::assignFromLine: other is not a ColumnConst of the same type");

        if (rowCount == 0) {
            return;
        }

        _value = otherCol->_value;
    }

    void set(const T& value) { _value = value; }

    void dump(std::ostream& out) const override {
        DebugDump::dump(out, _value);
    }

    T& getRaw() { return _value; }
    const T& getRaw() const { return _value; }

    static consteval auto staticKind() { return _staticKind; }

private:
    T _value;

    static constexpr auto _staticKind = ColumnKind::getColumnKind<ColumnConst<T>>();
};

}
