#pragma once

#include <cstdint>
#include <string_view>
#include <variant>

#include "metadata/PropertyType.h"

namespace db {

class ValueVariant {
private:
    template <typename Tuple>
    struct TupleToVariant;

    template <typename... Types>
    struct TupleToVariant<std::tuple<Types...>> {
        using Type = std::variant<Types...>;

        template <typename T>
        static constexpr bool ExactlyOnce = std::__detail::__variant::__exactly_once<T, Types...>;
    };

public:
    using Types = std::tuple<
        std::monostate,
        uint64_t,
        int64_t,
        double,
        std::string_view,
        bool,
        CustomBool>;

    using VariantType = TupleToVariant<Types>::Type;

    ValueVariant()
        : _value(std::monostate {}) {
    }

    ~ValueVariant() = default;

    ValueVariant(const ValueVariant&) = default;
    ValueVariant(ValueVariant&&) noexcept = default;
    ValueVariant& operator=(const ValueVariant&) = default;
    ValueVariant& operator=(ValueVariant&&) noexcept = default;

    explicit ValueVariant(VariantType value)
        : _value(value) {
    }

    bool isNull() const {
        return std::holds_alternative<std::monostate>(_value);
    }

    VariantType& getRaw() {
        return _value;
    }

    const VariantType& getRaw() const {
        return _value;
    }

    template <typename T>
    T& get() {
        return std::get<T>(_value);
    }

    template <typename T>
    const T& get() const {
        return std::get<T>(_value);
    }

    template <typename T>
    T* getIf() {
        return std::get_if<T>(&_value);
    }

    template <typename T>
    const T* getIf() const {
        return std::get_if<T>(&_value);
    }

    bool operator==(const ValueVariant& other) const {
        return _value == other._value;
    }

    template <typename T>
    bool operator==(T other) const {
        static_assert(TupleToVariant<Types>::template ExactlyOnce<T>,
                      "T is not part of the ValueVariant's Types");

        if (const T* p = getIf<T>()) {
            return *p == other;
        }

        return false;
    }

    bool operator==(const std::string& other) const {
        if (const std::string_view* p = getIf<std::string_view>()) {
            return *p == other;
        }

        return false;
    }

    friend bool operator==(const ValueVariant& left, const std::optional<ValueVariant>& right) {
        if (!right.has_value()) {
            return false;
        }

        const auto& r = right.value();

        return left == r;
    }

    template <typename T>
    friend bool operator==(const ValueVariant& left, const std::optional<T>& right) {
        static_assert(TupleToVariant<Types>::template ExactlyOnce<T>,
                      "T is not part of the ValueVariant's Types");

        if (!right.has_value()) {
            return false;
        }

        if (const T* p = left.getIf<T>()) {
            return *p == right.value();
        }

        return false;
    }

    template <typename T>
    friend bool operator==(const std::optional<T>& left, const ValueVariant& right) {
        static_assert(TupleToVariant<Types>::template ExactlyOnce<T>,
                      "T is not part of the ValueVariant's Types");

        if (!left.has_value()) {
            return false;
        }

        if (const T* p = right.getIf<T>()) {
            return left.value() == *p;
        }

        return false;
    }

private:
    VariantType _value;
};

}
