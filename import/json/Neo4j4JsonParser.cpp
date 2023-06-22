#include "Neo4j4JsonParser.h"
#include "BioAssert.h"
#include "BioLog.h"
#include "DB.h"
#include "Edge.h"
#include "EdgeType.h"
#include "MsgImport.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "Property.h"
#include "PropertyType.h"
#include "TimerStat.h"
#include "Writeback.h"

#include <nlohmann/json.hpp>

using JsonType = nlohmann::detail::value_t;
using JsonObject = nlohmann::basic_json<>;

const static std::unordered_map<std::string, db::ValueType> neo4jTypes = {
    {"String",         db::ValueType {db::ValueType::ValueKind::VK_STRING}  },
    {"StringArray",    db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
    {"Double",         db::ValueType {db::ValueType::ValueKind::VK_DECIMAL} },
    {"DoubleArray",    db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
    {"Boolean",        db::ValueType {db::ValueType::ValueKind::VK_BOOL}    },
    {"BooleanArray",   db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
    {"LocalDate",      db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
    {"LocalDateArray", db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
    {"Integer",        db::ValueType {db::ValueType::ValueKind::VK_INT}     },
    {"IntegerArray",   db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
    {"Long",           db::ValueType {db::ValueType::ValueKind::VK_UNSIGNED}},
    {"LongArray",      db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
    {"Date",           db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
    {"DateArray",      db::ValueType {db::ValueType::ValueKind::VK_INVALID} },
};

const static std::unordered_map<std::string, std::string> stlTypes = {
    {typeid(std::string).name(), "string"         },
    {typeid(bool).name(),        "boolean"        },
    {typeid(double).name(),      "number_float"   },
    {typeid(int64_t).name(),     "number_integer" },
    {typeid(uint64_t).name(),    "number_unsigned"},
};

const static std::unordered_map<JsonType, std::string> jsonTypes = {
    {JsonType::string,          "string"         },
    {JsonType::null,            "null"           },
    {JsonType::array,           "array"          },
    {JsonType::binary,          "binary"         },
    {JsonType::object,          "object"         },
    {JsonType::boolean,         "boolean"        },
    {JsonType::discarded,       "discarded"      },
    {JsonType::number_float,    "number_float"   },
    {JsonType::number_integer,  "number_integer" },
    {JsonType::number_unsigned, "number_unsigned"},
};

inline db::Network* Neo4j4JsonParser::getOrCreateNetwork() {
    const db::StringRef netName = _db->getString("Neo4jNetwork");
    db::Network* net = _wb->createNetwork(netName);
    if (!net) {
        net = _db->getNetwork(netName);
    }
    return net;
}

inline db::NodeType* Neo4j4JsonParser::getOrCreateNodeType(db::StringRef name) {
    db::NodeType* obj = _wb->createNodeType(name);
    if (!obj) {
        obj = _db->getNodeType(name);
    }

    return obj;
}

template <db::SupportedType T>
static bool isExpectedType(const JsonObject& value) {
    if constexpr (std::is_same<T, std::string>()) {
        return value.type() == JsonType::string;
    } else if constexpr (std::is_same<T, int64_t>()) {
        return value.type() == JsonType::number_integer
            || value.type() == JsonType::number_unsigned;
    } else if constexpr (std::is_same<T, uint64_t>()) {
        return value.type() == JsonType::number_unsigned;
    } else if constexpr (std::is_same<T, double>()) {
        return value.type() == JsonType::number_float;
    } else if constexpr (std::is_same<T, bool>()) {
        return value.type() == JsonType::boolean;
    } else {
        []<bool flag = false>() {
            static_assert(flag,
                          "Support for a new type was added, but not included "
                          "in this method. Implement it to allow compilation");
        }
        ();
    }
}

template <db::SupportedType T>
static T convert(const JsonObject& value) {
    if constexpr (std::is_same<T, std::string>()) {
        return value.dump();

    } else if constexpr (std::is_same<T, int64_t>()) {
        return std::stoi(value.dump());

    } else if constexpr (std::is_same<T, uint64_t>()) {
        std::string val = value.dump();
        if (std::stoi(val) < 0) {
            throw std::bad_cast {};
        }
        return std::stoul(val);

    } else if constexpr (std::is_same<T, double>()) {
        return std::stof(value.dump());

    } else if constexpr (std::is_same<T, bool>()) {
        return static_cast<bool>(std::stoi(value.dump()));

    } else {
        []<bool flag = false>() {
            static_assert(flag,
                          "Support for a new type was added, but not included "
                          "in this method. Implement it to allow compilation");
        }
        ();
    }
}

template <db::SupportedType T>
static T castFromJsonType(JsonParsingStats& stats, const JsonObject& value) {
    T toReturn;

    if (!isExpectedType<T>(value)) {
        toReturn = convert<T>(value);
        stats.nodePropWarnings++;
    } else {
        toReturn = value.get<T>();
    }

    return toReturn;
}

struct PropertyContext {
    JsonParsingStats& stats;
    const JsonObject& val;
    db::PropertyType* propType {nullptr};
    db::DBEntity* entity {nullptr};
    db::Writeback* wb {nullptr};
    const std::string& propName;
};

template <db::SupportedType T>
static void handleProperty(const PropertyContext& c) {
    try {
        T value;
        value = castFromJsonType<T>(c.stats, c.val);

        db::Property prop {
            c.propType,
            db::Value::create<T>(std::move(value)),
        };

        if (!c.wb->setProperty(c.entity, prop)) {
            c.stats.illformedNodeProps += 1;
        }
    } catch (const std::exception& e) {
        const std::string& jsonType = jsonTypes.at(c.val.type());
        const std::string& stlType = stlTypes.at(typeid(T).name());
        const std::string val = c.val.dump() + jsonType + stlType;

        if (c.stats.propErrors.find(val) == c.stats.propErrors.end()) {
            Log::BioLog::log(msg::ERROR_JSON_INCORRECT_TYPE()
                             << c.val.dump()
                             << c.propName
                             << jsonType
                             << stlType);
            c.stats.propErrors.emplace(std::move(val));
        }
        c.stats.nodePropErrors++;
    }
}

Neo4j4JsonParser::Neo4j4JsonParser(db::DB* db, JsonParsingStats& stats)
    : _db(db),
      _stats(stats),
      _wb(new db::Writeback(_db))
{
}

Neo4j4JsonParser::~Neo4j4JsonParser() {
    delete _wb;
}

bool Neo4j4JsonParser::parseStats(const std::string& data) {
    TimerStat timer {"JSON Parser: parsing Neo4J stats"};
    if (!_reducedOutput) {
        Log::BioLog::log(msg::INFO_NEO4J_READING_STATS());
    }

    const auto json = nlohmann::json::parse(data);
    const auto& row = json["results"].front()["data"].front()["row"];
    _stats.nodeCount = row.at(0).get<size_t>();
    _stats.edgeCount = row.at(1).get<size_t>();

    return true;
}

bool Neo4j4JsonParser::parseNodeProperties(const std::string& data) {
    TimerStat timer {"JSON Parser: parsing Neo4J node properties"};
    if (!_reducedOutput) {
        Log::BioLog::log(msg::INFO_NEO4J_READING_NODE_PROPERTIES());
    }

    const auto json = nlohmann::json::parse(data);
    const auto& results = json["results"].front()["data"];

    for (const auto& record : results) {
        const auto& row = record["row"];

        const db::StringRef ntName = _db->getString(row.at(0));
        db::NodeType* nt = getOrCreateNodeType(ntName);
        msgbioassert(
            neo4jTypes.find(row.at(3).at(0)) != neo4jTypes.end(),
            (row.at(3).at(0).get<std::string>() + " type is not supported")
                .c_str());

        const db::StringRef propName = _db->getString(row.at(2));
        const db::ValueType valType = neo4jTypes.find(row.at(3).at(0))->second;

        if (valType == db::ValueType::VK_INVALID) {
            _stats.unsupportedNodeProps += 1;
            continue;
        }

        if (!_wb->addPropertyType(nt, propName, valType)) {
            Log::BioLog::echo(
                "Problem with property type ["
                + propName + "] for node type [" + nt->getName() + "]");
            _stats.nodePropErrors += 1;
        }
    }

    return true;
}

bool Neo4j4JsonParser::parseNodes(const std::string& data) {
    TimerStat timer {"JSON Parser: parsing Neo4J nodes"};
    db::Network* net = getOrCreateNetwork();

    if (!_reducedOutput) {
        Log::BioLog::log(msg::INFO_NEO4J_READING_NODES());
    }
    const auto json = nlohmann::json::parse(data);
    const auto& results = json["results"].front()["data"];
    std::string label;

    // for each row
    for (const auto& record : results) {
        const auto& row = record["row"];
        label = "";

        // Getting NodeType
        for (const JsonObject& ntAliasStr : row.at(0)) {
            label += ":`" + ntAliasStr.get<std::string>() + "`";
        }
        const db::StringRef ntName = _db->getString(label);

        // If the NodeType does not exist, it means it was not created while
        // gathering node properties (due to having no properties or only
        // unsupported ones)
        db::NodeType* nt = getOrCreateNodeType(ntName);

        // Creating node
        const size_t nodeId = row.at(1).get<uint64_t>();

        const db::StringRef nodeName = _db->getString(std::to_string(nodeId));
        db::Node* n = _wb->createNode(net, nt, nodeName);
        _nodeIdMap.emplace(nodeId, n->getIndex());

        // For each <PropertyTypeName, value> pair
        for (const auto& [key, val] : row.at(2).items()) {
            const db::StringRef propName = _db->getString(key);
            db::PropertyType* propType = nt->getPropertyType(propName);

            // if the propType is invalid, it means the PropertyType was not
            // registered due to invalid (unsupported) type
            if (!propType)
                continue;

            PropertyContext context = {
                .stats = _stats,
                .val = val,
                .propType = propType,
                .entity = n,
                .wb = _wb,
                .propName = propName.getSharedString()->getString(),
            };

            switch (propType->getValueType().getKind()) {
                case db::ValueType::VK_STRING: {
                    handleProperty<std::string>(context);
                    break;
                }

                case db::ValueType::VK_INT: {
                    handleProperty<int64_t>(context);
                    break;
                }

                case db::ValueType::VK_DECIMAL: {
                    handleProperty<double>(context);
                    break;
                }

                case db::ValueType::VK_BOOL: {
                    handleProperty<bool>(context);
                    break;
                }

                case db::ValueType::VK_UNSIGNED: {
                    handleProperty<uint64_t>(context);
                    break;
                }

                default: {
                    // Something went wront
                    Log::BioLog::echo("FATAL ERROR, SHOULD NOT OCCUR: Invalid "
                                      "property type: "
                                      + propName);
                    _stats.nodePropErrors += 1;
                    break;
                }
            }
        }

        _stats.parsedNodes++;
    }

    return true;
}

bool Neo4j4JsonParser::parseEdgeProperties(const std::string& data) {
    TimerStat timer {"JSON Parser: parsing Neo4J edge properties"};
    if (!_reducedOutput) {
        Log::BioLog::log(msg::INFO_NEO4J_READING_EDGE_PROPERTIES());
    }

    const auto json = nlohmann::json::parse(data);
    const auto& results = json["results"].front()["data"];
    std::string sourceLabel;
    std::string targetLabel;

    // Each row is 1 property + 1 source type + 1 target type.
    // Thus at each row, we might have to create a new edge
    // type, add a source/target to an existing one, or add a
    // property
    for (const auto& record : results) {
        const auto& row = record["row"];
        const db::StringRef etName = _db->getString(row.at(0));
        sourceLabel = "";
        targetLabel = "";

        // Getting source NodeType
        for (const JsonObject& ntAliasStr : row.at(1)) {
            sourceLabel += ":`" + ntAliasStr.get<std::string>() + "`";
        }

        db::NodeType* sourceNt = _db->getNodeType(_db->getString(sourceLabel));
        if (!sourceNt) {
            Log::BioLog::log(msg::ERROR_NEO4J_PROBLEM_WITH_NODETYPE()
                             << sourceLabel);
            return false;
        }

        // Getting target NodeType
        for (const JsonObject& ntAliasStr : row.at(2)) {
            targetLabel += ":`" + ntAliasStr.get<std::string>() + "`";
        }

        db::NodeType* targetNt = _db->getNodeType(_db->getString(targetLabel));
        if (!targetNt) {
            Log::BioLog::log(msg::ERROR_NEO4J_PROBLEM_WITH_NODETYPE()
                             << targetLabel);
            return false;
        }

        db::EdgeType* et = _db->getEdgeType(etName);
        if (et) {
            _wb->addSourceNodeType(et, sourceNt);
            _wb->addTargetNodeType(et, targetNt);

        } else {
            et = _wb->createEdgeType(etName, sourceNt, targetNt);
        }

        // If the edge type has no property, we skip this part
        if (row.at(3).is_null()) {
            continue;
        }

        const db::StringRef propName = _db->getString(row.at(3));
        const db::ValueType valType = neo4jTypes.find(row.at(4).at(0))->second;

        if (valType == db::ValueType::VK_INVALID) {
            continue;
        }

        if (et->getPropertyType(propName)) {
            // PropertyType already added.
            // This loop step only added a source or target
            continue;
        }

        if (!_wb->addPropertyType(et, propName, valType)) {
            Log::BioLog::echo(
                "FATAL ERROR, SHOULD NOT OCCUR: Invalid property type: "
                + propName + "for edge type: " + et->getName());

            _stats.edgePropErrors += 1;
        }
    }

    return true;
}

bool Neo4j4JsonParser::parseEdges(const std::string& data) {
    TimerStat timer {"JSON Parser: parsing Neo4J edges"};
    db::Network* net = getOrCreateNetwork();

    if (!_reducedOutput) {
        Log::BioLog::log(msg::INFO_NEO4J_READING_EDGES());
    }
    const auto json = nlohmann::json::parse(data);
    const auto& results = json["results"].front()["data"];
    std::string label;

    // for each row
    for (const auto& record : results) {
        const auto& row = record["row"];

        const db::StringRef etName =
            _db->getString(":`" + row.at(0).get<std::string>() + "`");
        db::EdgeType* et = _db->getEdgeType(etName);
        db::Node* n1 = net->getNode(_nodeIdMap.at(row.at(1)));
        db::Node* n2 = net->getNode(_nodeIdMap.at(row.at(2)));

        if (!et) {
            // Edge type was not created when reading edge properties. This can
            // happend if the edge type has no properties
            // TODO change source/target here
            et = _wb->createEdgeType(etName, n1->getType(), n2->getType());
        }

        if (!et->hasSourceType(n1->getType())) {
            _wb->addSourceNodeType(et, n1->getType());
        }
        if (!et->hasTargetType(n2->getType())) {
            _wb->addTargetNodeType(et, n2->getType());
        }

        db::Edge* e = _wb->createEdge(et, n1, n2);

        // For each <PropertyTypeName, value> pair
        for (const auto& [key, val] : row.at(3).items()) {
            const db::StringRef propName = _db->getString(key);
            db::PropertyType* propType = et->getPropertyType(propName);

            // if the propType is invalid, it means the PropertyType was not
            // registered due to invalid (unsupported) type
            if (!propType)
                continue;

            PropertyContext context = {
                .stats = _stats,
                .val = val,
                .propType = propType,
                .entity = e,
                .wb = _wb,
                .propName = propName.getSharedString()->getString(),
            };

            switch (propType->getValueType().getKind()) {
                case db::ValueType::VK_STRING: {
                    handleProperty<std::string>(context);
                    break;
                }

                case db::ValueType::VK_INT: {
                    handleProperty<int64_t>(context);
                    break;
                }

                case db::ValueType::VK_DECIMAL: {
                    handleProperty<double>(context);
                    break;
                }

                case db::ValueType::VK_BOOL: {
                    handleProperty<bool>(context);
                    break;
                }

                case db::ValueType::VK_UNSIGNED: {
                    handleProperty<uint64_t>(context);
                    break;
                }

                default: {
                    // Something went wront
                    Log::BioLog::echo("FATAL ERROR, SHOULD NOT OCCUR: Invalid "
                                      "property type: "
                                      + propName);

                    _stats.nodePropErrors += 1;
                    break;
                }
            }
        }

        _stats.parsedEdges++;
    }

    return true;
}
