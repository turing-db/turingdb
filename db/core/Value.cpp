// Copyright 2023 Turing Biosystems Ltd.

#include "Value.h"

using namespace db;

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
