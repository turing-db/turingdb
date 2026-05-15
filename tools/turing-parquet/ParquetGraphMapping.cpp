#include "ParquetGraphMapping.h"

#include <stddef.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetPropertyAnalysis.h"

using namespace db;

namespace {

bool isPrimitive(ParquetJsonValueType type) {
    const bool boolType = type == ParquetJsonValueType::BOOLEAN;
    const bool integerType = type == ParquetJsonValueType::INTEGER;
    const bool floatType = type == ParquetJsonValueType::FLOAT;
    const bool stringType = type == ParquetJsonValueType::STRING;
    return boolType || integerType || floatType || stringType;
}

ParquetTuringType toTuringType(ParquetJsonValueType type) {
    switch (type) {
        case ParquetJsonValueType::BOOLEAN:
            return ParquetTuringType::BOOLEAN;
        break;
        case ParquetJsonValueType::INTEGER:
            return ParquetTuringType::INTEGER;
        break;
        case ParquetJsonValueType::FLOAT:
            return ParquetTuringType::FLOAT;
        break;
        case ParquetJsonValueType::STRING:
            return ParquetTuringType::STRING;
        break;
        default:
            return ParquetTuringType::STRING;
        break;
    }
}

void addRawJsonProperty(ParquetGraphLabel& parent,
                        const std::string& name,
                        bool isNullable) {
    ParquetGraphProperty& property = parent.addProperty();
    property.setName(name);
    property.setType(ParquetTuringType::STRING);
    property.setNullable(isNullable);
    property.setRawJson(true);
}

void addPrimitiveProperty(ParquetGraphLabel& parent,
                          const std::string& name,
                          ParquetJsonValueType valueType,
                          bool isNullable) {
    ParquetGraphProperty& property = parent.addProperty();
    property.setName(name);
    property.setType(toTuringType(valueType));
    property.setNullable(isNullable);
}

std::string joinPath(const std::string& parentPath, const std::string& name) {
    if (parentPath.empty()) {
        return name;
    }
    return parentPath + "." + name;
}

void addPropertyEntry(ParquetGraphLabel& parent,
                      const std::string& name,
                      const ParquetPropertyType& propertyType,
                      const std::string& parentPath,
                      ParquetGraphMapping& mapping);

void populateFromObjectKeys(ParquetGraphLabel& label,
                            const ParquetPropertyType::SubPropertyMap& subProperties,
                            const std::string& objectPath,
                            ParquetGraphMapping& mapping) {
    for (const auto& entry : subProperties) {
        addPropertyEntry(label, entry.first, *entry.second, objectPath, mapping);
    }
}

void populateFromArrayElement(ParquetGraphLabel& label,
                              const ParquetPropertyType* elementType,
                              const std::string& arrayPath,
                              ParquetGraphMapping& mapping) {
    if (elementType == nullptr) {
        addRawJsonProperty(label, "value", false);
        mapping.addWarning(fmt::format("'{}' arrays were always empty — element type unknown",
                                       arrayPath));
        return;
    }

    if (elementType->isMixed()) {
        addRawJsonProperty(label, "value", elementType->isNullable());
        mapping.addWarning(fmt::format("'{}[]' has mixed element types — emitted as raw JSON string",
                                       arrayPath));
        return;
    }

    const ParquetJsonValueType elementValueType = elementType->getValueType();

    if (elementValueType == ParquetJsonValueType::NIL) {
        addRawJsonProperty(label, "value", true);
        mapping.addWarning(fmt::format("'{}[]' elements were always null — type unknown, emitted as raw JSON string",
                                       arrayPath));
        return;
    }

    if (isPrimitive(elementValueType)) {
        addPrimitiveProperty(label, "value", elementValueType, elementType->isNullable());
        return;
    }

    if (elementValueType == ParquetJsonValueType::OBJECT) {
        const std::string innerPath = arrayPath + "[]";
        populateFromObjectKeys(label, elementType->getSubProperties(), innerPath, mapping);
        return;
    }

    if (elementValueType == ParquetJsonValueType::ARRAY) {
        addRawJsonProperty(label, "value", elementType->isNullable());
        mapping.addWarning(fmt::format("'{}[]' contains nested arrays — emitted as raw JSON string",
                                       arrayPath));
        return;
    }
}

void addPropertyEntry(ParquetGraphLabel& parent,
                      const std::string& name,
                      const ParquetPropertyType& propertyType,
                      const std::string& parentPath,
                      ParquetGraphMapping& mapping) {
    const std::string path = joinPath(parentPath, name);
    const ParquetJsonValueType valueType = propertyType.getValueType();

    if (propertyType.isMixed()) {
        addRawJsonProperty(parent, name, propertyType.isNullable());
        mapping.addWarning(fmt::format("'{}' has mixed types — emitted as raw JSON string", path));
        return;
    }

    if (valueType == ParquetJsonValueType::NIL) {
        addRawJsonProperty(parent, name, true);
        mapping.addWarning(fmt::format("'{}' was always null — type unknown, emitted as raw JSON string",
                                       path));
        return;
    }

    if (isPrimitive(valueType)) {
        addPrimitiveProperty(parent, name, valueType, propertyType.isNullable());
        return;
    }

    if (valueType == ParquetJsonValueType::OBJECT) {
        ParquetGraphLabel& subLabel = parent.addSubLabel();
        subLabel.setName(name);
        subLabel.setCardinality(ParquetEdgeCardinality::ONE);
        subLabel.setNullable(propertyType.isNullable());
        populateFromObjectKeys(subLabel, propertyType.getSubProperties(), path, mapping);
        return;
    }

    if (valueType == ParquetJsonValueType::ARRAY) {
        ParquetGraphLabel& subLabel = parent.addSubLabel();
        subLabel.setName(name);
        subLabel.setCardinality(ParquetEdgeCardinality::MANY);
        subLabel.setNullable(propertyType.isNullable());
        populateFromArrayElement(subLabel, propertyType.getElementType(), path, mapping);
        return;
    }
}

}

ParquetGraphProperty::ParquetGraphProperty() {
}

ParquetGraphProperty::~ParquetGraphProperty() {
}

ParquetGraphLabel::ParquetGraphLabel() {
}

ParquetGraphLabel::~ParquetGraphLabel() {
}

ParquetGraphProperty& ParquetGraphLabel::addProperty() {
    auto entry = std::make_unique<ParquetGraphProperty>();
    ParquetGraphProperty* raw = entry.get();
    _properties.push_back(std::move(entry));
    return *raw;
}

ParquetGraphLabel& ParquetGraphLabel::addSubLabel() {
    auto entry = std::make_unique<ParquetGraphLabel>();
    ParquetGraphLabel* raw = entry.get();
    _subLabels.push_back(std::move(entry));
    return *raw;
}

ParquetGraphMapping::ParquetGraphMapping() {
}

ParquetGraphMapping::~ParquetGraphMapping() {
}

const char* ParquetGraphMapping::toString(ParquetTuringType type) {
    switch (type) {
        case ParquetTuringType::BOOLEAN:
            return "boolean";
        break;
        case ParquetTuringType::INTEGER:
            return "integer";
        break;
        case ParquetTuringType::FLOAT:
            return "float";
        break;
        case ParquetTuringType::STRING:
            return "string";
        break;
    }
    return "?";
}

const char* ParquetGraphMapping::toString(ParquetEdgeCardinality cardinality) {
    switch (cardinality) {
        case ParquetEdgeCardinality::ONE:
            return "1:1";
        break;
        case ParquetEdgeCardinality::MANY:
            return "1:N";
        break;
    }
    return "?";
}

void ParquetGraphMapping::buildFrom(const ParquetPropertyAnalysis& analysis,
                                    const std::string& columnName,
                                    ParquetGraphMapping& mapping) {
    mapping.setColumnName(columnName);
    for (const auto& entry : analysis.getPropertyTypes()) {
        addPropertyEntry(mapping.getRoot(), entry.first, *entry.second, "", mapping);
    }
}
