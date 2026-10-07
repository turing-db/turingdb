#include "ParameterValue.h"

#include "BioAssert.h"

using namespace db;

ParameterValue::ParameterValue()
{
}

ParameterValue::~ParameterValue() {
}

bool ParameterValue::isNull() const {
    return std::holds_alternative<std::monostate>(_value);
}

ValueType ParameterValue::getType() const {
    bioassert(!isNull(), "A null parameter has no value type");

    if (std::holds_alternative<int64_t>(_value)) {
        return ValueType::Int64;
    } else if (std::holds_alternative<double>(_value)) {
        return ValueType::Double;
    } else if (std::holds_alternative<bool>(_value)) {
        return ValueType::Bool;
    } else if (std::holds_alternative<std::string>(_value)) {
        return ValueType::String;
    } else if (std::holds_alternative<List>(_value)) {
        return ValueType::List;
    } else {
        return ValueType::Map;
    }
}

int64_t ParameterValue::getInt64() const {
    bioassert(std::holds_alternative<int64_t>(_value), "Parameter is not an Int64");
    return std::get<int64_t>(_value);
}

double ParameterValue::getDouble() const {
    bioassert(std::holds_alternative<double>(_value), "Parameter is not a Double");
    return std::get<double>(_value);
}

bool ParameterValue::getBool() const {
    bioassert(std::holds_alternative<bool>(_value), "Parameter is not a Bool");
    return std::get<bool>(_value);
}

const std::string& ParameterValue::getString() const {
    bioassert(std::holds_alternative<std::string>(_value), "Parameter is not a String");
    return std::get<std::string>(_value);
}

const ParameterValue::List& ParameterValue::getList() const {
    bioassert(std::holds_alternative<List>(_value), "Parameter is not a List");
    return std::get<List>(_value);
}

const ParameterValue::Map& ParameterValue::getMap() const {
    bioassert(std::holds_alternative<Map>(_value), "Parameter is not a Map");
    return std::get<Map>(_value);
}

void ParameterValue::setNull() {
    _value.emplace<std::monostate>();
}

void ParameterValue::setInt64(int64_t value) {
    _value.emplace<int64_t>(value);
}

void ParameterValue::setDouble(double value) {
    _value.emplace<double>(value);
}

void ParameterValue::setBool(bool value) {
    _value.emplace<bool>(value);
}

void ParameterValue::setString(std::string_view value) {
    _value.emplace<std::string>(value);
}

ParameterValue::List& ParameterValue::setList() {
    return _value.emplace<List>();
}

ParameterValue::Map& ParameterValue::setMap() {
    return _value.emplace<Map>();
}
