#include "JsonlParser.h"

#include <nlohmann/json.hpp>

#include "ID.h"
#include "JsonlImportResult.h"
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
    Profile profile {"JsonlParser::parse"};

    std::string line;
    json obj;
    size_t lineNumber = 0;
    std::unordered_map<uint64_t, NodeID> nodeIDs;
    CommitBuilder* tip = change.getTip();
    DataPartBuilder& builder = tip->getCurrentBuilder();
    MetadataBuilder& metadataBuilder = tip->metadata();
    std::string ptName;
    LabelSet labelset;

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
                    for (const auto& [key, value] : properties->items()) {
                        ptName = key;
                        ValueType vt {};

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
                            builder.addNodeProperty<types::Double>(nodeID, pt._id, value.get<double>());
                        } else if (value.is_boolean()) {
                            builder.addNodeProperty<types::Bool>(nodeID, pt._id, value.get<bool>());
                        } else if (value.is_number()) {
                            builder.addNodeProperty<types::Int64>(nodeID, pt._id, value.get<int64_t>());
                        } else if (value.is_string()) {
                            builder.addNodeProperty<types::String>(nodeID, pt._id, value.get<std::string_view>());
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

                const auto srcIDIt = start->find("id");
                const auto tgtIDIt = end->find("id");

                if (srcIDIt == start->end()) {
                    return JsonlImportError::result(JsonlImportErrorType::MISSING_EDGE_SRC_ID, lineNumber);
                }

                if (tgtIDIt == end->end()) {
                    return JsonlImportError::result(JsonlImportErrorType::MISSING_EDGE_TGT_ID, lineNumber);
                }

                const NodeID srcNodeID = nodeIDs.at(parseEntityID(*srcIDIt));
                const NodeID tgtNodeID = nodeIDs.at(parseEntityID(*tgtIDIt));

                [[maybe_unused]] const EdgeRecord& edge = builder.addEdge(edgeTypeID, srcNodeID, tgtNodeID);

                if (properties != obj.end()) {
                    for (const auto& [key, value] : properties->items()) {
                        ptName = key;
                        ValueType vt {};

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
