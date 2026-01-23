#pragma once

#include <map>
#include <optional>
#include <span>

#include "EnumToString.h"
#include "ID.h"
#include "SupportedType.h"

namespace db {

enum class ValueType : uint8_t {
    Invalid = 0,
    Int64,
    UInt64,
    Double,
    String,
    Bool,

    _SIZE,
};

using ValueTypeName = EnumToString<ValueType>::Create<
    EnumStringPair<ValueType::Invalid, "Invalid">,
    EnumStringPair<ValueType::Int64, "Int64">,
    EnumStringPair<ValueType::UInt64, "UInt64">,
    EnumStringPair<ValueType::Double, "Double">,
    EnumStringPair<ValueType::String, "String">,
    EnumStringPair<ValueType::Bool, "Bool">>;

struct CustomBool {
    CustomBool() = default;
    CustomBool(bool v)
        : _boolean(v)
    {
    }

    CustomBool& operator=(bool v) {
        _boolean = v;
        return *this;
    }

    bool operator==(const CustomBool& other) const {
        return _boolean == other._boolean;
    }

    bool operator||(const CustomBool& other) const {
        return _boolean || other._boolean;
    }

    bool operator!() const {
        return !_boolean;
    }

    operator bool() const { return _boolean; }

    bool _boolean;
};

struct PropertyType {
    PropertyTypeID _id;
    ValueType _valueType {};

    bool isValid() const { return _id.isValid(); }

    bool operator==(const PropertyType& other) const {
        return _id.getValue() == other._id.getValue() && _valueType == other._valueType ;
    }
};

namespace types {

struct Int64 : public PropertyType {
    using Primitive = int64_t;
    using MandatorySpan = std::span<const Primitive>;
    using OptionalSpan = std::span<const std::optional<Primitive>>;
    static constexpr auto _valueType = ValueType::Int64;
};

struct UInt64 : public PropertyType {
    using Primitive = uint64_t;
    using MandatorySpan = std::span<const Primitive>;
    using OptionalSpan = std::span<const std::optional<Primitive>>;
    static constexpr auto _valueType = ValueType::UInt64;
};

struct Double : public PropertyType {
    using Primitive = double;
    using MandatorySpan = std::span<const Primitive>;
    using OptionalSpan = std::span<const std::optional<Primitive>>;
    static constexpr auto _valueType = ValueType::Double;
};

struct String : public PropertyType {
    using Primitive = std::string_view;
    using MandatorySpan = std::span<const Primitive>;
    using OptionalSpan = std::span<const std::optional<Primitive>>;
    static constexpr auto _valueType = ValueType::String;
};

struct Bool : public PropertyType {
    using Primitive = CustomBool;
    using MandatorySpan = std::span<const Primitive>;
    using OptionalSpan = std::span<const std::optional<Primitive>>;
    static constexpr auto _valueType = ValueType::Bool;
};

}

template <typename T>
concept TrivialSupportedType = SupportedType<T> && !std::same_as<T, types::String>;

enum class PropertyImportance : uint8_t {
    Mandatory = 0,
    Optional,
};

struct PropertyTypeInfo {
    ValueType _type {};
    size_t _count = 0;
};

using PropertyTypeInfos = std::map<PropertyTypeID, PropertyTypeInfo>;

}

template <>
struct std::hash<db::CustomBool> {
    std::size_t operator()(const db::CustomBool& cb) const noexcept {
        return std::hash<bool> {}(cb._boolean);
    }
};

// XXX TODO: Better hash function
template <>
struct std::hash<db::PropertyType> {
    std::size_t operator()(const db::PropertyType& pt) const noexcept {
        return std::hash<unsigned short> {}(pt._id.getValue())
             + std::hash<uint8_t> {}(static_cast<uint8_t>(pt._valueType));
    }
};

