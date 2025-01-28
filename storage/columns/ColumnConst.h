#pragma once

#include "Column.h"
#include "ColumnKind.h"

#include "DebugDump.h"

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

    Status assign(const Column* other) override {
        const ColumnConst<T>* otherCol = dynamic_cast<const ColumnConst<T>*>(other);
        if (!otherCol) {
            return Status(DBError::COLUMNS_WITH_DIFFERENT_TYPE);
        }

        _value = otherCol->_value;

        return Status::ok();
    }

    void set(const T& value) { _value = value; }

    void dump() const override {
        DebugDump::dump(_value);
    }

    T& getRaw() { return _value; }
    const T& getRaw() const { return _value; }

    static consteval auto staticKind() { return _staticKind; }

private:
    T _value;

    static constexpr auto _staticKind = ColumnKind::getColumnKind<ColumnConst<T>>();
};

}
