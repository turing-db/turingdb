#include "ParquetPropertyAnalysis.h"

using namespace db;

ParquetPropertyAnalysis::ParquetPropertyAnalysis() {
}

ParquetPropertyAnalysis::~ParquetPropertyAnalysis() {
}

void ParquetPropertyAnalysis::recordValue(ParquetJsonValueType type) {
    ++_typeCounts[static_cast<size_t>(type)];
    ++_totalCount;
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
