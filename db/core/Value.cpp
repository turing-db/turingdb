// Copyright 2023 Turing Biosystems Ltd.

#include <array>
#include <string_view>
#include <typeinfo>
#include <unordered_map>

#include "Value.h"
#include "BioAssert.h"

using namespace db;

static constexpr std::array<std::string_view, ValueType::_SIZE> valueKindNames {
    "Invalid",
    "Int",
    "Unsigned",
    "Bool",
    "Decimal",
    "StringRef",
    "String",
};

static_assert(valueKindNames[(size_t)ValueType::VK_INVALID] == "Invalid");
static_assert(valueKindNames[(size_t)ValueType::VK_INT] == "Int");
static_assert(valueKindNames[(size_t)ValueType::VK_UNSIGNED] == "Unsigned");
static_assert(valueKindNames[(size_t)ValueType::VK_BOOL] == "Bool");
static_assert(valueKindNames[(size_t)ValueType::VK_STRING_REF] == "StringRef");
static_assert(valueKindNames[(size_t)ValueType::VK_STRING] == "String");
static_assert(valueKindNames.size() == 7);

template <db::SupportedType T>
consteval ValueType::ValueKind getKind() {
    if constexpr (std::is_same<T, std::string>()) {
        return db::ValueType::VK_STRING;
    } else if constexpr (std::is_same<T, StringRef>()) {
        return db::ValueType::VK_STRING_REF;
    } else if constexpr (std::is_same<T, int64_t>()) {
        return db::ValueType::VK_INT;
    } else if constexpr (std::is_same<T, uint64_t>()) {
        return db::ValueType::VK_UNSIGNED;
    } else if constexpr (std::is_same<T, double>()) {
        return db::ValueType::VK_DECIMAL;
    } else if constexpr (std::is_same<T, bool>()) {
        return db::ValueType::VK_BOOL;
    } else {
        []<bool flag = false>() {
            static_assert(flag,
                          "Support for a new type was added, but not included "
                          "here. Implement it to allow compilation");
        }
        ();
    }
}

Value::Value()
{
}

Value::~Value() {
}

template <>
Value Value::create(int64_t&& data) {
    return createInt(data);
}

template <>
Value Value::create(uint64_t&& data) {
    return createUnsigned(data);
}

template <>
Value Value::create(bool&& data) {
    return createBool(data);
}

template <>
Value Value::create(double&& data) {
    return createDouble(data);
}

template <>
Value Value::create(StringRef&& data) {
    return createStringRef(data);
}

template <>
Value Value::create(std::string&& data) {
    return createString(std::forward<std::string>(data));
}

Value Value::createInt(int64_t data) {
    Value val;
    val._type = ValueType::VK_INT;
    val._data = data;
    return val;
}

Value Value::createUnsigned(uint64_t data) {
    Value val;
    val._type = ValueType::VK_UNSIGNED;
    val._data = data;
    return val;
}

Value Value::createBool(bool data) {
    Value val;
    val._type = ValueType::VK_BOOL;
    val._data = data;
    return val;
}

Value Value::createDouble(double data) {
    Value val;
    val._type = ValueType::VK_DECIMAL;
    val._data = data;
    return val;
}

Value Value::createStringRef(StringRef data) {
    Value val;
    val._type = ValueType::VK_STRING_REF;
    val._data = data;
    return val;
}

Value Value::createString(std::string&& data) {
    Value val;
    val._type = ValueType::VK_STRING;
    val._data = std::forward<std::string>(data);
    return val;
}

template <db::SupportedType T>
const T& Value::get() const {
    const T* val = std::get_if<T>(&_data);

    msgbioassert(val, [&]() {
        std::string error = "Non matching value types: ValueType is '";

        error += valueKindNames[_type.getKind()];
        error += "' but tried to access it as '";
        error += valueKindNames[getKind<T>()];
        error += "'";

        return error;
    }().c_str());

    return *val;
}

int64_t Value::getInt() const {
    return get<int64_t>();
}

uint64_t Value::getUint() const {
    return get<uint64_t>();
}

bool Value::getBool() const {
    return get<bool>();
}

double Value::getDouble() const {
    return get<double>();
}

StringRef Value::getStringRef() const {
    return get<StringRef>();
}

const std::string& Value::getString() const {
    return get<std::string>();
}
