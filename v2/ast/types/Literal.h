#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <variant>

namespace db {

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
    using ValueType = std::variant<bool, int64_t, double, std::string_view, char, MapLiteral*, std::nullopt_t>;

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

private:
    ValueType _value;
};

}
