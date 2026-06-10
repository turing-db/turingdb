#include "JsonlParser.h"

#include <nlohmann/json.hpp>
#include <unordered_map>

#include "ID.h"
#include "JsonlImportResult.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "writers/DataPartBuilder.h"

#include "Profiler.h"
#include "writers/MetadataBuilder.h"

using json = nlohmann::json;

namespace {

[[nodiscard]] uint64_t getInteger(const std::string_view& str) {
    if (str.empty()) {
        throw std::invalid_argument("Empty ID string");
    }

    uint64_t v = 0;
    const char* begin = str.data();
    const char* end = str.data() + str.size();
    const auto res = std::from_chars(begin, end, v, 10);

    if (res.ec == std::errc::result_out_of_range) {
        throw std::out_of_range("Too large ID value");
    } else if (res.ec == std::errc::invalid_argument) {
        throw std::invalid_argument("Invalid ID value");
    } else if (res.ptr != end) {
        throw std::invalid_argument("ID contains non-digit characters");
    }

    return v;
};

[[nodiscard]] uint64_t parseEntityID(const json& id) {
    if (id.is_number()) {
        return id.get<uint64_t>();
    } else if (id.is_string()) {
        const auto idStr = id.get<std::string_view>();
        return getInteger(idStr);
    } else {
        throw std::invalid_argument("Invalid ID type");
    }
}

}

namespace db {

JsonlImportResult<void> JsonlParser::parse(ChangeAccessor& change, std::istream& stream) {
    Profile profile("JsonlParser::parse");

    std::string line;
    json obj;
    size_t lineNumber = 0;
    std::unordered_map<uint64_t, NodeID> nodeIDs;
    CommitBuilder* tip = change.getTip();
    DataPartBuilder& builder = tip->getCurrentBuilder();
    MetadataBuilder& metadataBuilder = tip->metadata();
    std::string ptName;
    LabelSet labelset;
    std::unordered_map<PropertyTypeID, size_t> embPropSizes;

    while (std::getline(stream, line)) {
        ++lineNumber;

        // If empty line, skip
        if (line.empty()) {
            continue;
        }

        // If contains only whitespaces, skip
        if (line.find_first_not_of(" \t\r") == std::string::npos) {
            continue;
        }

        try {
            obj = json::parse(line);

            const auto type = obj.find("type");
            const auto id = obj.find("id");

            if (type == obj.end()) {
                return JsonlImportError::result(JsonlImportErrorType::MISSING_ENTITY_TYPE, lineNumber);
            }

            if (id == obj.end()) {
                return JsonlImportError::result(JsonlImportErrorType::MISSING_ENTITY_TYPE, lineNumber);
            }

            if (type->get<std::string_view>() == "node") {
                const auto labels = obj.find("labels");
                const auto properties = obj.find("properties");

                if (labels == obj.end()) {
                    return JsonlImportError::result(JsonlImportErrorType::MISSING_LABELS, lineNumber);
                }

                labelset = LabelSet {};
                for (const auto& label : *labels) {
                    LabelID labelID = metadataBuilder.getOrCreateLabel(label.get<std::string_view>());
                    labelset.set(labelID);
                }

                if (labelset.empty()) {
                    return JsonlImportError::result(JsonlImportErrorType::MISSING_LABELS, lineNumber);
                }

                const NodeID nodeID = builder.addNode(labelset);
                nodeIDs.emplace(parseEntityID(*id), nodeID);

                if (properties != obj.end()) {
                    // Reusable storage for loading embedding properties
                    std::vector<float> embBacking;

                    for (const auto& [key, value] : properties->items()) {
                        ptName = key;
                        ValueType vt = ValueType::Invalid;

                        if (value.is_number_float()) {
                            vt = ValueType::Double;
                        } else if (value.is_boolean()) {
                            vt = ValueType::Bool;
                        } else if (value.is_number()) {
                            vt = ValueType::Int64;
                        } else if (value.is_array()) {
                            vt = ValueType::Embedding;
                        } else {
                            vt = ValueType::String;
                        }

                        PropertyType pt = metadataBuilder.getOrCreatePropertyType(ptName, vt);
                        if (pt._valueType != vt) {
                            ptName += fmt::format(" ({})", ValueTypeName::value(vt));
                            pt = metadataBuilder.getOrCreatePropertyType(ptName, vt);
                        }

                        if (value.is_number_float()) {
                            builder.addNodeProperty<types::Double>(nodeID, pt._id, value.get<double>());
                        } else if (value.is_boolean()) {
                            builder.addNodeProperty<types::Bool>(nodeID, pt._id, value.get<bool>());
                        } else if (value.is_number()) {
                            builder.addNodeProperty<types::Int64>(nodeID, pt._id, value.get<int64_t>());
                        } else if (value.is_string()) {
                            builder.addNodeProperty<types::String>(nodeID, pt._id, value.get<std::string_view>());
                        } else if (value.is_array()) {
                            const size_t sz = value.size();

                            // Embeddings must be at least dimension 1
                            if (sz == 0) {
                                return JsonlImportError::result(
                                    JsonlImportErrorType::NON_EMB_ARRAY, lineNumber);
                            }

                            { // All embeddings need be the same size
                                const auto findIt = embPropSizes.find(pt._id);
                                const bool alreadyRegistered = findIt != (end(embPropSizes));
                                if (alreadyRegistered) {
                                    if (sz != findIt->second) {
                                        return JsonlImportError::result(
                                            JsonlImportErrorType::MISMATCH_EMB_DIM,
                                            lineNumber);
                                    }
                                } else {
                                    embPropSizes.emplace(pt._id, sz);
                                }
                            }

                            embBacking.clear();
                            embBacking.reserve(sz);

                            for (const json& e : value) {
                                if (!e.is_number()) {
                                    return JsonlImportError::result(
                                        JsonlImportErrorType::NON_EMB_ARRAY, lineNumber);
                                }
                                const float v = e.get<float>(); // Will work for any numeric type
                                embBacking.push_back(v);
                            }

                            builder.addNodeProperty<types::Embedding>(nodeID, pt._id, embBacking);
                        } else {
                            builder.addNodeProperty<types::String>(nodeID, pt._id, value.dump());
                        }
                    }
                }

            } else if (type->get<std::string_view>() == "relationship") {
                const auto edgeType = obj.find("label");
                const auto properties = obj.find("properties");
                const auto start = obj.find("start");
                const auto end = obj.find("end");

                if (edgeType == obj.end()) {
                    return JsonlImportError::result(JsonlImportErrorType::MISSING_EDGE_TYPE, lineNumber);
                }

                if (start == obj.end()) {
                    return JsonlImportError::result(JsonlImportErrorType::MISSING_EDGE_TYPE, lineNumber);
                }

                if (end == obj.end()) {
                    return JsonlImportError::result(JsonlImportErrorType::MISSING_EDGE_TYPE, lineNumber);
                }

                EdgeTypeID edgeTypeID = metadataBuilder.getOrCreateEdgeType(edgeType->get<std::string_view>());

                NodeID srcNodeID;
                NodeID tgtNodeID;

                if (start->is_object()) {
                    const auto srcIDIt = start->find("id");

                    if (srcIDIt == start->end()) {
                        return JsonlImportError::result(JsonlImportErrorType::MISSING_EDGE_SRC_ID, lineNumber);
                    }

                    srcNodeID = nodeIDs.at(parseEntityID(*srcIDIt));
                } else {
                    srcNodeID = parseEntityID(*start);
                }

                if (end->is_object()) {
                    const auto tgtIDIt = end->find("id");

                    if (tgtIDIt == end->end()) {
                        return JsonlImportError::result(JsonlImportErrorType::MISSING_EDGE_TGT_ID, lineNumber);
                    }

                    tgtNodeID = nodeIDs.at(parseEntityID(*tgtIDIt));
                } else {
                    tgtNodeID = parseEntityID(*end);
                }

                const EdgeRecord& edge = builder.addEdge(edgeTypeID, srcNodeID, tgtNodeID);

                if (properties != obj.end()) {
                    for (const auto& [key, value] : properties->items()) {
                        ptName = key;
                        ValueType vt = ValueType::Invalid;

                        if (value.is_number_float()) {
                            vt = ValueType::Double;
                        } else if (value.is_boolean()) {
                            vt = ValueType::Bool;
                        } else if (value.is_number()) {
                            vt = ValueType::Int64;
                        } else {
                            vt = ValueType::String;
                        }

                        PropertyType pt = metadataBuilder.getOrCreatePropertyType(ptName, vt);
                        if (pt._valueType != vt) {
                            ptName += fmt::format(" ({})", ValueTypeName::value(vt));
                            pt = metadataBuilder.getOrCreatePropertyType(ptName, vt);
                        }

                        if (value.is_number_float()) {
                            builder.addEdgeProperty<types::Double>(edge, pt._id, value.get<double>());
                        } else if (value.is_boolean()) {
                            builder.addEdgeProperty<types::Bool>(edge, pt._id, value.get<bool>());
                        } else if (value.is_number()) {
                            builder.addEdgeProperty<types::Int64>(edge, pt._id, value.get<int64_t>());
                        } else if (value.is_string()) {
                            builder.addEdgeProperty<types::String>(edge, pt._id, value.get<std::string_view>());
                        } else {
                            builder.addEdgeProperty<types::String>(edge, pt._id, value.dump());
                        }
                    }
                }
            }

        } catch (const std::exception& e) {
            return JsonlImportError::result(JsonlImportErrorType::JSONL_PARSE_ERROR, lineNumber, e.what());
        }
    }

    return {};
}

}
