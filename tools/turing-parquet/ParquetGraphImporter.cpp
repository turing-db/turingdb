#include "ParquetGraphImporter.h"

#include <ctype.h>

#include <span>

#include <nlohmann/json.hpp>
#include <parquet/types.h>
#include <spdlog/fmt/fmt.h>

#include "Graph.h"
#include "Path.h"
#include "ParquetReader.h"

#include "metadata/PropertyType.h"

#include "ParquetEdgeRowVisitor.h"
#include "ParquetGraphMapping.h"
#include "ParquetNodeRowVisitor.h"
#include "ParquetSchema.h"

#include "TuringException.h"

using namespace db;

namespace {

// Return true if the schema has a top-level field with this name and it is a BYTE_ARRAY primitive.
bool isTopLevelByteArray(const ParquetSchema& schema, const std::string& name) {
    const ParquetSchemaField& root = schema.getRoot();
    const size_t childCount = root.getChildCount();
    for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
        const ParquetSchemaField& child = root.getChild(childIndex);
        if (child.getName() != name) {
            continue;
        }
        if (child.isGroup()) {
            return false;
        }
        return child.getPrimitiveType() == ParquetPrimitiveType::BYTE_ARRAY;
    }
    return false;
}

// Throw if the schema lacks a top-level BYTE_ARRAY column with this name; `role` describes it in the message.
void requireByteArrayColumn(const ParquetSchema& schema,
                            const std::string& name,
                            const std::string& role) {
    if (!isTopLevelByteArray(schema, name)) {
        throw TuringException(fmt::format(
            "Required {} column '{}' is missing or not a top-level BYTE_ARRAY",
            role,
            name));
    }
}

// Find a child sub-label by name on `parent`, returning nullptr when not present.
const ParquetGraphLabel* findSubLabel(const ParquetGraphLabel& parent,
                                      const std::string& name) {
    for (const auto& subLabel : parent.getSubLabels()) {
        if (subLabel->getName() == name) {
            return subLabel.get();
        }
    }
    return nullptr;
}

// Find a property by name on `label`, returning nullptr when not present.
const ParquetGraphProperty* findProperty(const ParquetGraphLabel& label,
                                         const std::string& name) {
    for (const auto& property : label.getProperties()) {
        if (property->getName() == name) {
            return property.get();
        }
    }
    return nullptr;
}

// Convert an arbitrary string to UPPER_SNAKE_CASE for use as an edge relation type.
std::string toUpperSnake(std::string_view value) {
    std::string result;
    result.reserve(value.size());
    bool lastWasUnderscore = true;
    for (const char c : value) {
        if (isalnum(static_cast<unsigned char>(c))) {
            result.push_back(static_cast<char>(toupper(static_cast<unsigned char>(c))));
            lastWasUnderscore = false;
        } else if (!lastWasUnderscore) {
            result.push_back('_');
            lastWasUnderscore = true;
        }
    }
    while (!result.empty() && result.back() == '_') {
        result.pop_back();
    }
    return result;
}

// Map a `ParquetGraphProperty` to the corresponding TuringDB column `ValueType` (raw-JSON stores as String).
ValueType propertyValueType(const ParquetGraphProperty& property) {
    if (property.isRawJson()) {
        return ValueType::String;
    }
    switch (property.getType()) {
        case ParquetTuringType::BOOLEAN:
            return ValueType::Bool;
        break;
        case ParquetTuringType::INTEGER:
            return ValueType::Int64;
        break;
        case ParquetTuringType::FLOAT:
            return ValueType::Double;
        break;
        case ParquetTuringType::STRING:
            return ValueType::String;
        break;
    }
    return ValueType::String;
}

}

ParquetGraphImporter::ParquetGraphImporter(Graph* graph,
                                           JobSystem* jobSystem,
                                           const ParquetGraphMapping& propertyMapping,
                                           const std::string& propertyColumn,
                                           const std::string& edgeTypeColumn)
    : _writer(graph, jobSystem),
    _propertyMapping(propertyMapping),
    _propertyColumn(propertyColumn),
    _edgeTypeColumn(edgeTypeColumn)
{
}

ParquetGraphImporter::~ParquetGraphImporter() {
}

void ParquetGraphImporter::importNodeFile(const fs::Path& path,
                                          const ParquetSchema& schema) {
    requireByteArrayColumn(schema, "id", "node id");
    requireByteArrayColumn(schema, "label", "node label");
    requireByteArrayColumn(schema, _propertyColumn, "node properties");

    ParquetNodeRowVisitor visitor(schema, _propertyColumn, *this);
    ParquetReader reader(path, visitor);
    while (reader.nextChunk()) {
    }
}

void ParquetGraphImporter::importEdgeFile(const fs::Path& path,
                                          const ParquetSchema& schema) {
    requireByteArrayColumn(schema, "from", "edge from");
    requireByteArrayColumn(schema, "to", "edge to");
    requireByteArrayColumn(schema, _edgeTypeColumn, "edge type");
    requireByteArrayColumn(schema, _propertyColumn, "edge properties");

    ParquetEdgeRowVisitor visitor(schema, _edgeTypeColumn, _propertyColumn, *this);
    ParquetReader reader(path, visitor);
    while (reader.nextChunk()) {
    }
}

void ParquetGraphImporter::finalize() {
    _writer.submit();
}

std::string ParquetGraphImporter::resolvePropertyName(const std::string& baseName,
                                                     ValueType valueType) {
    const auto it = _propertyTypeByName.find(baseName);
    if (it == _propertyTypeByName.end()) {
        _propertyTypeByName.emplace(baseName, valueType);
        return baseName;
    }
    if (it->second == valueType) {
        return baseName;
    }
    // Collision: append the value-type tag so the second-seen variant of the
    // same name gets its own property type.
    std::string mangled = baseName + " (" + std::string(ValueTypeName::value(valueType)) + ")";
    _propertyTypeByName.emplace(mangled, valueType);
    return mangled;
}

void ParquetGraphImporter::addScalarNodeProperty(NodeID node,
                                                 const ParquetGraphProperty& property,
                                                 const nlohmann::json& value) {
    if (value.is_null()) {
        return;
    }
    const ValueType valueType = propertyValueType(property);
    const std::string name = resolvePropertyName(property.getName(), valueType);

    if (property.isRawJson()) {
        _writer.addNodeProperty<types::String>(node, name, value.dump());
        return;
    }
    switch (property.getType()) {
        case ParquetTuringType::BOOLEAN:
            if (value.is_boolean()) {
                _writer.addNodeProperty<types::Bool>(node, name, CustomBool(value.get<bool>()));
            }
        break;
        case ParquetTuringType::INTEGER:
            if (value.is_number_integer() || value.is_number_unsigned()) {
                _writer.addNodeProperty<types::Int64>(node, name, value.get<int64_t>());
            }
        break;
        case ParquetTuringType::FLOAT:
            if (value.is_number()) {
                _writer.addNodeProperty<types::Double>(node, name, value.get<double>());
            }
        break;
        case ParquetTuringType::STRING:
            if (value.is_string()) {
                _writer.addNodeProperty<types::String>(node, name, value.get<std::string_view>());
            }
        break;
    }
}

void ParquetGraphImporter::addScalarEdgeProperty(const EdgeRecord& edge,
                                                 const ParquetGraphProperty& property,
                                                 const nlohmann::json& value) {
    if (value.is_null()) {
        return;
    }
    const ValueType valueType = propertyValueType(property);
    const std::string name = resolvePropertyName(property.getName(), valueType);

    if (property.isRawJson()) {
        _writer.addEdgeProperty<types::String>(edge, name, value.dump());
        return;
    }
    switch (property.getType()) {
        case ParquetTuringType::BOOLEAN:
            if (value.is_boolean()) {
                _writer.addEdgeProperty<types::Bool>(edge, name, CustomBool(value.get<bool>()));
            }
        break;
        case ParquetTuringType::INTEGER:
            if (value.is_number_integer() || value.is_number_unsigned()) {
                _writer.addEdgeProperty<types::Int64>(edge, name, value.get<int64_t>());
            }
        break;
        case ParquetTuringType::FLOAT:
            if (value.is_number()) {
                _writer.addEdgeProperty<types::Double>(edge, name, value.get<double>());
            }
        break;
        case ParquetTuringType::STRING:
            if (value.is_string()) {
                _writer.addEdgeProperty<types::String>(edge, name, value.get<std::string_view>());
            }
        break;
    }
}

void ParquetGraphImporter::writeSubRecord(NodeID parent,
                                          std::string_view edgeType,
                                          const ParquetGraphLabel& label,
                                          const nlohmann::json& value) {
    if (!value.is_object()) {
        return;
    }

    const std::string& nodeLabel = label.getInferredLabel().empty()
                                   ? label.getName()
                                   : label.getInferredLabel();

    // Dedup key:
    //  - If the sub-record has an explicit `id` field, use it (id is treated
    //    as an authoritative natural key — two observations with the same id
    //    collapse, even if other scalar fields disagree slightly).
    //  - Otherwise if a wrapped scalar `value` is present, use it (same
    //    rationale for arrays-of-scalars like canonical_exons).
    //  - Otherwise fall back to a hash of the full canonical JSON content
    //    under the sub-record's label. nlohmann::json keeps object keys in
    //    std::map order so .dump() is canonical: byte-identical sub-records
    //    share a key and a single node. This catches structurally-identical
    //    sub-records that lack a natural id (e.g. SubcellularLocation, Url).
    std::string dedupKey;
    {
        const auto idIt = value.find("id");
        const auto valueIt = value.find("value");
        if (idIt != value.end() && !idIt->is_null()) {
            dedupKey = nodeLabel + ":id:" + idIt->dump();
        } else if (valueIt != value.end() && !valueIt->is_null()) {
            dedupKey = nodeLabel + ":value:" + valueIt->dump();
        } else {
            dedupKey = nodeLabel + ":content:" + value.dump();
        }
    }

    const auto it = _subRecordByKey.find(dedupKey);
    if (it != _subRecordByKey.end()) {
        _writer.addEdge(edgeType, parent, it->second);
        ++_dedupedReferenceCount;
        return;
    }

    const NodeID sub = _writer.addNode({std::string_view(nodeLabel)});
    ++_subRecordCount;
    _subRecordByKey.emplace(dedupKey, sub);

    _writer.addEdge(edgeType, parent, sub);

    for (const auto& [key, child] : value.items()) {
        const ParquetGraphProperty* const property = findProperty(label, key);
        if (property != nullptr) {
            addScalarNodeProperty(sub, *property, child);
            continue;
        }
        const ParquetGraphLabel* const subLabel = findSubLabel(label, key);
        if (subLabel == nullptr) {
            continue;
        }
        const std::string nestedEdgeType = "HAS_" + toUpperSnake(key);
        if (child.is_object()) {
            writeSubRecord(sub, nestedEdgeType, *subLabel, child);
        } else if (child.is_array()) {
            for (const auto& element : child) {
                if (element.is_object()) {
                    writeSubRecord(sub, nestedEdgeType, *subLabel, element);
                } else if (!element.is_null()) {
                    const nlohmann::json wrapped = {{"value", element}};
                    writeSubRecord(sub, nestedEdgeType, *subLabel, wrapped);
                }
            }
        }
    }
}

void ParquetGraphImporter::onNodeRow(std::string_view id,
                                     std::string_view label,
                                     std::string_view propertiesJson) {
    if (id.empty() || label.empty()) {
        return;
    }

    const NodeID node = _writer.addNode({label});
    _writer.addNodeProperty<types::String>(node, resolvePropertyName("id", ValueType::String),
                                           std::string_view(id));
    _externalIdToNodeId.emplace(std::string(id), node);
    ++_nodeCount;

    if (propertiesJson.empty()) {
        return;
    }
    const auto json = nlohmann::json::parse(propertiesJson, nullptr,
                                            /*allow_exceptions=*/false);
    if (json.is_discarded() || !json.is_object()) {
        return;
    }

    const ParquetGraphLabel& root = _propertyMapping.getRoot();
    for (const auto& [key, value] : json.items()) {
        const ParquetGraphProperty* const property = findProperty(root, key);
        if (property != nullptr) {
            addScalarNodeProperty(node, *property, value);
            continue;
        }
        const ParquetGraphLabel* const subLabel = findSubLabel(root, key);
        if (subLabel == nullptr) {
            continue;
        }
        const std::string edgeType = "HAS_" + toUpperSnake(key);
        if (value.is_object()) {
            writeSubRecord(node, edgeType, *subLabel, value);
        } else if (value.is_array()) {
            for (const auto& element : value) {
                if (element.is_object()) {
                    writeSubRecord(node, edgeType, *subLabel, element);
                } else if (!element.is_null()) {
                    const nlohmann::json wrapped = {{"value", element}};
                    writeSubRecord(node, edgeType, *subLabel, wrapped);
                }
            }
        }
    }
}

NodeID ParquetGraphImporter::resolveOrCreateStubNode(std::string_view externalId) {
    const auto it = _externalIdToNodeId.find(std::string(externalId));
    if (it != _externalIdToNodeId.end()) {
        return it->second;
    }
    const NodeID stub = _writer.addNode({std::string_view("External")});
    _writer.addNodeProperty<types::String>(stub, resolvePropertyName("id", ValueType::String),
                                          std::string_view(externalId));
    _externalIdToNodeId.emplace(std::string(externalId), stub);
    ++_stubNodeCount;
    return stub;
}

void ParquetGraphImporter::onEdgeRow(std::string_view fromId,
                                     std::string_view toId,
                                     std::string_view relation,
                                     std::string_view propertiesJson,
                                     bool undirected) {
    if (fromId.empty() || toId.empty() || relation.empty()) {
        ++_skippedEdgeCount;
        return;
    }

    const NodeID fromNode = resolveOrCreateStubNode(fromId);
    const NodeID toNode = resolveOrCreateStubNode(toId);

    const EdgeRecord edge = _writer.addEdge(relation, fromNode, toNode);
    ++_edgeCount;

    if (propertiesJson.empty()) {
        return;
    }
    const auto json = nlohmann::json::parse(propertiesJson, nullptr,
                                            /*allow_exceptions=*/false);
    if (json.is_discarded() || !json.is_object()) {
        return;
    }

    const ParquetGraphLabel& root = _propertyMapping.getRoot();
    for (const auto& [key, value] : json.items()) {
        const ParquetGraphProperty* const property = findProperty(root, key);
        if (property != nullptr) {
            addScalarEdgeProperty(edge, *property, value);
        }
    }
}
