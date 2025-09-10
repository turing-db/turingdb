#pragma once

#include <cstdint>
#include <memory>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <variant>

namespace db::v2 {

class Expression;

class MapLiteral {
public:
    MapLiteral() = default;
    ~MapLiteral() = default;

    MapLiteral(const MapLiteral&) = default;
    MapLiteral(MapLiteral&&) = default;
    MapLiteral& operator=(const MapLiteral&) = default;
    MapLiteral& operator=(MapLiteral&&) = default;

    static std::unique_ptr<MapLiteral> create() {
        return std::make_unique<MapLiteral>();
    }

    void set(const std::string_view& key, Expression* value) {
        _map[key] = value;
    }

    std::unordered_map<std::string_view, Expression*>::const_iterator begin() const {
        return _map.begin();
    }

    std::unordered_map<std::string_view, Expression*>::const_iterator end() const {
        return _map.end();
    }

private:
    std::unordered_map<std::string_view, Expression*> _map;
};

class Literal {
public:
    enum class Type {
        Null,
        Bool,
        Integer,
        Double,
        String,
        Char,
        Map,
    };

    using ValueType = std::variant<std::monostate, bool, int64_t, double, std::string_view, char, MapLiteral*>;

    Literal() = default;

    explicit Literal(const ValueType& value)
        : _value(value)
    {
    }

    template <typename T>
    bool is() const {
        return std::holds_alternative<T>(_value);
    }

    template <typename T>
    const T* as() const {
        return std::get_if<T>(&_value);
    }

    template <typename T>
    T* as() {
        return std::get_if<T>(&_value);
    }

    const auto& value() const {
        return _value;
    }

    template <typename T>
    static consteval Type type() {
        constexpr size_t value = VariantTypeIndex<T, ValueType>::value;
        constexpr size_t max = std::numeric_limits<std::underlying_type_t<Type>>::max();
        static_assert(value < max, "Type index out of range. Type not handled in enum");

        return static_cast<Type>(VariantTypeIndex<T, ValueType>::value);
    }

    Type type() const {
        return static_cast<Type>(_value.index());
    }

private:
    ValueType _value;

    template <typename>
    struct Tag {};

    template <typename T, typename V>
    struct VariantTypeIndex;

    template <typename T, typename... Ts>
    struct VariantTypeIndex<T, std::variant<Ts...>>
        : std::integral_constant<size_t, std::variant<Tag<Ts>...>(Tag<T>()).index()>
    {
    };
};

static_assert(Literal::type<std::monostate>() == Literal::Type::Null);
static_assert(Literal::type<bool>() == Literal::Type::Bool);
static_assert(Literal::type<int64_t>() == Literal::Type::Integer);
static_assert(Literal::type<double>() == Literal::Type::Double);
static_assert(Literal::type<std::string_view>() == Literal::Type::String);
static_assert(Literal::type<char>() == Literal::Type::Char);
static_assert(Literal::type<MapLiteral*>() == Literal::Type::Map);

}
