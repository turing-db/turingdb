#include "ParquetPropertyAnalysis.h"

using namespace db;

ParquetPropertyType::ParquetPropertyType() {
}

ParquetPropertyType::~ParquetPropertyType() {
}

void ParquetPropertyType::recordValue(ParquetJsonValueType type) {
    ++_count;

    if (type == ParquetJsonValueType::NIL) {
        _isNullable = true;
        return;
    }

    if (!_hasNonNullValue) {
        _valueType = type;
        _hasNonNullValue = true;
    } else if (_valueType != type) {
        _isMixed = true;
    }
}

ParquetPropertyType& ParquetPropertyType::getOrCreateSubProperty(const std::string& name) {
    const auto it = _subProperties.find(name);
    if (it != _subProperties.end()) {
        return *it->second;
    }

    auto entry = std::make_unique<ParquetPropertyType>();
    entry->setName(name);
    ParquetPropertyType* raw = entry.get();
    _subProperties.emplace(name, std::move(entry));
    return *raw;
}

ParquetPropertyType& ParquetPropertyType::getOrCreateElementType() {
    if (_elementType == nullptr) {
        _elementType = std::make_unique<ParquetPropertyType>();
    }
    return *_elementType;
}

ParquetPropertyAnalysis::ParquetPropertyAnalysis() {
}

ParquetPropertyAnalysis::~ParquetPropertyAnalysis() {
}

ParquetPropertyType& ParquetPropertyAnalysis::getOrCreatePropertyType(const std::string& name) {
    const auto it = _propertyTypes.find(name);
    if (it != _propertyTypes.end()) {
        return *it->second;
    }

    auto entry = std::make_unique<ParquetPropertyType>();
    entry->setName(name);
    ParquetPropertyType* raw = entry.get();
    _propertyTypes.emplace(name, std::move(entry));
    return *raw;
}

void ParquetPropertyAnalysis::recordValue(ParquetJsonValueType type) {
    ++_typeCounts[static_cast<size_t>(type)];
    ++_totalCount;
}

void ParquetPropertyAnalysis::addTypeCount(ParquetJsonValueType type, size_t count) {
    _typeCounts[static_cast<size_t>(type)] += count;
    _totalCount += count;
}

void ParquetPropertyAnalysis::recordArrayPreview(const std::string& preview) {
    if (_arrayPreviews.size() < MAX_PREVIEWS) {
        _arrayPreviews.push_back(preview);
    }
}

void ParquetPropertyAnalysis::recordObjectPreview(const std::string& preview) {
    if (_objectPreviews.size() < MAX_PREVIEWS) {
        _objectPreviews.push_back(preview);
    }
}

size_t ParquetPropertyAnalysis::getTypeCount(ParquetJsonValueType type) const {
    return _typeCounts[static_cast<size_t>(type)];
}

const char* ParquetPropertyAnalysis::toString(ParquetJsonValueType type) {
    switch (type) {
        case ParquetJsonValueType::NIL:
            return "null";
        break;
        case ParquetJsonValueType::BOOLEAN:
            return "boolean";
        break;
        case ParquetJsonValueType::INTEGER:
            return "integer";
        break;
        case ParquetJsonValueType::FLOAT:
            return "float";
        break;
        case ParquetJsonValueType::STRING:
            return "string";
        break;
        case ParquetJsonValueType::ARRAY:
            return "array";
        break;
        case ParquetJsonValueType::OBJECT:
            return "object";
        break;
    }
    return "?";
}
