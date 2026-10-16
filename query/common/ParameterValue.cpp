#include "ParameterValue.h"

#include <utility>
#include <vector>

#include "BioAssert.h"

using namespace db;

ParameterValue::ParameterValue()
{
}

ParameterValue::ParameterValue(const ParameterValue& other)
{
    copyFrom(other);
}

ParameterValue::ParameterValue(ParameterValue&& other) noexcept
{
    _value.swap(other._value);
}

ParameterValue::~ParameterValue() {
    std::vector<Variant> worklist;
    detachContainer(worklist);

    while (!worklist.empty()) {
        Variant container;
        container.swap(worklist.back());
        worklist.pop_back();

        detachChildren(container, worklist);
    }
}

ParameterValue& ParameterValue::operator=(const ParameterValue& other) {
    if (this != &other) {
        ParameterValue copy(other);
        _value.swap(copy._value);
    }

    return *this;
}

ParameterValue& ParameterValue::operator=(ParameterValue&& other) noexcept {
    if (this != &other) {
        ParameterValue previous;
        previous._value.swap(_value);
        _value.swap(other._value);
    }

    return *this;
}

bool ParameterValue::holdsContainer() const {
    return std::holds_alternative<List>(_value) || std::holds_alternative<Map>(_value);
}

void ParameterValue::copyFrom(const ParameterValue& other) {
    using Item = std::pair<const ParameterValue*, ParameterValue*>;

    std::vector<Item> worklist;
    worklist.emplace_back(&other, this);

    while (!worklist.empty()) {
        const auto [source, target] = worklist.back();
        worklist.pop_back();

        if (const List* sourceList = std::get_if<List>(&source->_value)) {
            List& targetList = target->_value.emplace<List>(sourceList->size());

            for (size_t index = 0; index < targetList.size(); index++) {
                worklist.emplace_back(&(*sourceList)[index], &targetList[index]);
            }
        } else if (const Map* sourceMap = std::get_if<Map>(&source->_value)) {
            Map& targetMap = target->_value.emplace<Map>();

            for (const auto& [key, entry] : *sourceMap) {
                worklist.emplace_back(&entry, &targetMap[key]);
            }
        } else {
            target->_value = source->_value;
        }
    }
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

std::string_view ParameterValue::getTypeName() const {
    return isNull() ? "Null" : ValueTypeName::value(getType());
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

void ParameterValue::detachContainer(std::vector<Variant>& worklist) {
    if (holdsContainer()) {
        worklist.emplace_back().swap(_value);
    }
}

void ParameterValue::detachChildren(Variant& container, std::vector<Variant>& worklist) {
    if (List* list = std::get_if<List>(&container)) {
        for (ParameterValue& element : *list) {
            element.detachContainer(worklist);
        }
    } else if (Map* map = std::get_if<Map>(&container)) {
        for (auto& [key, entry] : *map) {
            entry.detachContainer(worklist);
        }
    }
}
