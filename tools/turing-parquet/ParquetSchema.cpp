#include "ParquetSchema.h"

using namespace db;

ParquetSchemaField::ParquetSchemaField() {
}

ParquetSchemaField::~ParquetSchemaField() {
}

ParquetSchemaField& ParquetSchemaField::addChild() {
    _children.push_back(std::make_unique<ParquetSchemaField>());
    return *_children.back();
}

ParquetSchema::ParquetSchema() {
}

ParquetSchema::~ParquetSchema() {
}

const char* ParquetSchema::toString(ParquetFieldRepetition repetition) {
    switch (repetition) {
        case ParquetFieldRepetition::REQUIRED:
            return "required";
        break;
        case ParquetFieldRepetition::OPTIONAL:
            return "optional";
        break;
        case ParquetFieldRepetition::REPEATED:
            return "repeated";
        break;
        case ParquetFieldRepetition::UNDEFINED:
            return "undefined";
        break;
    }
    return "undefined";
}

const char* ParquetSchema::toString(ParquetPrimitiveType primitiveType) {
    switch (primitiveType) {
        case ParquetPrimitiveType::BOOLEAN:
            return "BOOLEAN";
        break;
        case ParquetPrimitiveType::INT32:
            return "INT32";
        break;
        case ParquetPrimitiveType::INT64:
            return "INT64";
        break;
        case ParquetPrimitiveType::INT96:
            return "INT96";
        break;
        case ParquetPrimitiveType::FLOAT:
            return "FLOAT";
        break;
        case ParquetPrimitiveType::DOUBLE:
            return "DOUBLE";
        break;
        case ParquetPrimitiveType::BYTE_ARRAY:
            return "BYTE_ARRAY";
        break;
        case ParquetPrimitiveType::FIXED_LEN_BYTE_ARRAY:
            return "FIXED_LEN_BYTE_ARRAY";
        break;
    }
    return "UNKNOWN";
}
