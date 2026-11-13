#pragma once

#include "Column.h"
#include "ColumnKind.h"
#include "OptionalTraits.h"

#include "metadata/PropertyNull.h"

#include "DebugDump.h"
#include "BioAssert.h"
#include "NameOf.h"
#include "FatalException.h"

namespace db {

template <typename T>
class ColumnConst : public Column {
public:
    using ValueType = T;
    using UnwrappedType = OptionalTraits::unwrap_optional_t<T>;

    static constexpr bool IS_NULLABLE = OptionalTraits::is_optional_v<T>;
    static constexpr ContainerKind::Code BaseKind = ContainerKind::code<ColumnConst<T>>();

    ColumnConst()
        : Column(_staticKind),
        _empty(true)
    {
    }

    explicit ColumnConst(T&& value)
        : Column(_staticKind),
          _value(std::move(value))
    {
    }

    ~ColumnConst() override = default;

    ColumnConst(const ColumnConst&) = default;
    ColumnConst(ColumnConst&&) noexcept = default;

    ColumnConst& operator=(T&& value) requires (!IS_NULLABLE) { _value = std::move(value); _empty = false; return *this; }
    ColumnConst& operator=(const ColumnConst&) = default;
    ColumnConst& operator=(ColumnConst&&) noexcept = default;

    explicit operator T() const requires (!IS_NULLABLE) { return _value; }

    /// Subscript operators to match API of ColumnVector
    const T& operator[](size_t /*unused*/) const requires (!IS_NULLABLE) { return _value; }
    const T& at(size_t /*unused*/) const requires (!IS_NULLABLE) {
        if (_empty) {
            throw FatalException("ColumnConst::at called on empty column");
        }
        return _value;
    }

    bool empty() const { return _empty; }
    size_t size() const override { return _empty ? 0 : 1; }

    bool isValid(size_t row) const requires IS_NULLABLE { return _value.has_value(); }
    T get(size_t row) const requires IS_NULLABLE { return _value; }

    const UnwrappedType& getValue(size_t row) const requires IS_NULLABLE {
        if (_value.has_value()) {
            return *_value;
        }

        static const UnwrappedType nullValue {};
        return nullValue;
    }

    size_t getNullCount() const requires IS_NULLABLE { return (_empty || _value.has_value()) ? 0 : 1; }

    void clear() override { _empty = true; }

    void assign(const Column* other) override {
        const ColumnConst<T>* otherCol = dynamic_cast<const ColumnConst<T>*>(other);
        bioassert(otherCol, "ColumnConst::assign: other is not a ColumnConst of the same type");
        _value = otherCol->_value;
        _empty = otherCol->_empty;
    }

    void assignFromLine(const Column* other, size_t startLine, size_t rowCount) override {
        const ColumnConst<T>* otherCol = dynamic_cast<const ColumnConst<T>*>(other);
        bioassert(otherCol, "ColumnConst::assignFromLine: other is not a ColumnConst of the same type");

        if (rowCount == 0) {
            _empty = true;
            return;
        }

        _empty = false;
        _value = otherCol->_value;
    }

    void set(const T& value) requires (!IS_NULLABLE) { _value = value; _empty = false; }
    void set(T&& value) requires (!IS_NULLABLE) { _value = std::move(value); _empty = false; }

    void set(const UnwrappedType& value) requires IS_NULLABLE { _value = value; _empty = false; }
    void set(UnwrappedType&& value) requires IS_NULLABLE { _value = std::move(value); _empty = false; }

    void setOptional(const T& value) requires IS_NULLABLE { _value = value; _empty = false; }
    void setOptional(T&& value) requires IS_NULLABLE { _value = std::move(value); _empty = false; }

    void dump(std::ostream& out) const override {
        DebugDump::dump(out, _value);
    }

    T& getRaw() requires (!IS_NULLABLE) { return _value; }
    const T& getRaw() const requires (!IS_NULLABLE) { return _value; }

    static consteval auto staticKind() { return _staticKind; }

    ContainerKind::Code getContainerKind() const override { return ContainerKind::code<ColumnConst<T>>(); }
    InternalKind::Code getInternalKind() const override { return InternalKind::code<T>(); }

    std::string_view getTypeName() const override { return NameOf<ColumnConst<T>>::get(); }

private:
    T _value;
    bool _empty {false};

    static constexpr auto _staticKind = ColumnKind::code<ColumnConst<T>>();
};

template <>
ColumnConst<PropertyNull>::ColumnConst();

template <class T>
struct ColumnSupportFor<ColumnConst<T>> : std::true_type {};
}
