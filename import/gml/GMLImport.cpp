#include "GMLImport.h"

#include <spdlog/spdlog.h>

#include "DB.h"
#include "Edge.h"
#include "EdgeType.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "Property.h"
#include "Writeback.h"

#include "StringBuffer.h"
#include "StringToNumber.h"

using namespace db;

GMLImport::GMLImport(const StringBuffer* buffer,
                     db::DB* db,
                     db::Network* outNet)
    : _lexer(buffer->getData(), buffer->getSize()),
      _db(db),
      _wb(db),
      _outNet(outNet)
{
}

GMLImport::~GMLImport() {
}

bool GMLImport::run() {
    // Start parsing file
    _lexer.nextToken();

    const auto& token = _lexer.getToken();
    while (!token.isEnd() && !token.isError()) {
        if (!parseCommand()) {
            spdlog::error("Failed to parse GML command at line {}", _lexer.getLine());
            return false;
        }
        _lexer.nextToken();
    }

    return true;
}

bool GMLImport::parseCommand() {
    if (_lexer.getToken().getType() != GMLToken::TK_STRING) {
        spdlog::error("Unexpected token '{}' at line {}",
                      _lexer.getToken().getData(),
                      _lexer.getLine());
        return false;
    }

    const std::string_view keyword(_lexer.getToken().getData());
    if (keyword == "graph") {
        return parseGraphCommand();
    } else if (keyword == "node") {
        return parseNodeCommand();
    } else if (keyword == "edge") {
        return parseEdgeCommand();
    } else {
        return parseGenericCommand(keyword);
    }
}

bool GMLImport::parseNodeCommand() {
    _nodeProperties.clear();
    _insideNode = true;

    _lexer.nextToken();

    if (_lexer.getToken().getType() != GMLToken::TK_OSBRACK) {
        spdlog::error("Unexpected token '{}' at line {}",
                      _lexer.getToken().getData(),
                      _lexer.getLine());
        return false;
    }

    if (!parseList()) {
        return false;
    }

    // Get node ID and possibly node type name
    size_t nodeID = 0;
    std::string nodeTypeName;
    bool errorID = true;
    for (const auto& prop : _nodeProperties) {
        if (prop.first == "id") {
            nodeID = StringToNumber<size_t>(prop.second, errorID);
        } else if (prop.first == "type") {
            nodeTypeName = prop.second;
        }
    }

    if (errorID) {
        spdlog::error("Node ID not found at line {}", _lexer.getLine());
        return false;
    }

    if (nodeTypeName.empty()) {
        nodeTypeName = _outNet->getName().getSharedString()->getString() + "_GenericNode";
    }

    db::NodeType* nodeType = _db->getNodeType(_db->getString(nodeTypeName));
    if (!nodeType) {
        nodeType = _wb.createNodeType(_db->getString(nodeTypeName));
    }

    Node* node = _wb.createNode(_outNet, nodeType);
    if (!node) {
        spdlog::error("Failed to create node for line {}", _lexer.getLine());
        return false;
    }

    for (const auto& prop : _nodeProperties) {
        if (prop.first != "id") {
            const StringRef propName = _db->getString(std::string(prop.first));
            PropertyType* pt = _wb.addPropertyType(nodeType, propName, ValueType::VK_STRING);
            if (!pt) {
                pt = nodeType->getPropertyType(propName);
            }
            _wb.setProperty(node, Property(pt, Value::createString(std::string(prop.second))));
        }
    }

    _nodeIDs[nodeID] = node;
    _insideNode = false;

    return true;
}

bool GMLImport::parseEdgeCommand() {
    _edgeProperties.clear();
    _source = std::string_view();
    _target = std::string_view();

    _insideEdge = true;

    _lexer.nextToken();

    if (_lexer.getToken().getType() != GMLToken::TK_OSBRACK) {
        spdlog::error("Unexpected token '{}' at line {}",
                      _lexer.getToken().getData(),
                      _lexer.getLine());
        return false;
    }

    if (!parseList()) {
        return false;
    }

    if (_source.empty() || _target.empty()) {
        spdlog::error("No source node or target node at line {}", _lexer.getLine());
        return false;
    }

    size_t sourceID = 0;
    size_t targetID = 0;
    bool errorSource = true;
    bool errorTarget = true;
    sourceID = StringToNumber<size_t>(_source, errorSource);
    targetID = StringToNumber<size_t>(_target, errorTarget);

    if (errorSource) {
        spdlog::error("Impossible to convert id '{}' at line {}", _source, _lexer.getLine());
        return false;
    }

    if (errorTarget) {
        spdlog::error("Impossible to convert id '{}' at line {}", _target, _lexer.getLine());
        return false;
    }

    // Get edge type name if defined
    std::string edgeTypeName;
    for (const auto& prop : _edgeProperties) {
        if (prop.first == "type") {
            edgeTypeName = prop.second;
            break;
        }
    }

    if (edgeTypeName == "DBLinkage") {
        return parseDBLinkageCommand(sourceID, targetID);
    }

    // Get source and target nodes

    Node* sourceNode = getNodeFromID(sourceID);
    Node* targetNode = getNodeFromID(targetID);
    if (!sourceNode) {
        spdlog::error("Node ID {} not found for node at line {}",
                      sourceID,
                      _lexer.getLine());
        return false;
    }

    if (!targetNode) {
        spdlog::error("Node ID {} not found for node at line {}",
                      sourceID,
                      _lexer.getLine());
        return false;
    }

    if (edgeTypeName.empty()) {
        edgeTypeName = _outNet->getName().getSharedString()->getString() + "_GenericEdge";
    }

    db::EdgeType* edgeType = _db->getEdgeType(_db->getString(edgeTypeName));
    db::NodeType* srcType = sourceNode->getType();
    db::NodeType* tgtType = targetNode->getType();

    if (!edgeType) {
        edgeType = _wb.createEdgeType(
            _db->getString(edgeTypeName),
            srcType,
            tgtType);
    } else {
        // Make sure that the EdgeType supports the
        // src/tgt NodeType combo
        _wb.addSourceNodeType(edgeType, srcType);
        _wb.addTargetNodeType(edgeType, tgtType);
        // addXNodeType is a tryAdd so no need to check if
        // the edgeType has the X NodeType beforehand
    }

    // Create edge
    Edge* edge = _wb.createEdge(edgeType, sourceNode, targetNode);
    if (!edge) {
        spdlog::error("Failed to create edge at line {}", _lexer.getLine());
        return false;
    }

    for (const auto& prop : _edgeProperties) {
        if (prop.first != "id") {
            const StringRef propName = _db->getString(std::string(prop.first));
            PropertyType* pt = _wb.addPropertyType(edgeType, propName, ValueType::VK_STRING);
            if (!pt) {
                pt = edgeType->getPropertyType(propName);
            }
            _wb.setProperty(edge, Property(pt, Value::createString(std::string(prop.second))));
        }
    }

    _insideEdge = false;

    return true;
}

bool::GMLImport::parseDBLinkageCommand(size_t sourceID, size_t targetID) {
    // Get source and target networks
    std::string sourceNetName;
    std::string targetNetName;

    for (const auto& prop : _edgeProperties) {
        if (prop.first == "source_network") {
            sourceNetName = prop.second;
        }
        if (prop.first == "target_network") {
            targetNetName = prop.second;
        }
    }

    if (sourceNetName.empty()) {
        spdlog::error("Source network not specified");
        return false;
    }

    if (targetNetName.empty()) {
        spdlog::error("Target network not specified");
        return false;
    }

    Network* sourceNet = _db->getNetwork(_db->getString(sourceNetName));
    Network* targetNet = _db->getNetwork(_db->getString(targetNetName));
    if (!sourceNet) {
        spdlog::error("Network {} not found", sourceNetName);
        return false;
    }

    if (!targetNet) {
        spdlog::error("Network {} not found", targetNetName);
        return false;
    }

    // Here, sourceID should be an offseted id
    // Loop through all networks in the db. if net->getIndex() < sourceNet->getIndex()
    // we need to add the nodes.size() to the sourceID

    for (const Network* net: _db->networks()) {
        if (net->getIndex() < sourceNet->getIndex()) {
            sourceID += net->nodes().size();
        }
        if (net->getIndex() < targetNet->getIndex()) {
            targetID += net->nodes().size();
        }
    }
    Node* sourceNode = sourceNet->getNode((DBIndex)sourceID);
    Node* targetNode = targetNet->getNode((DBIndex)targetID);
    if (!sourceNode) {
        spdlog::error("Node ID {} not found for node at line {}",
                      sourceID, _lexer.getLine());
        return false;
    }

    if (!targetNode) {
        spdlog::error("Node ID {} not found for node at line {}",
                      targetID, _lexer.getLine());
        return false;
    }

    db::EdgeType* edgeType = _db->getEdgeType(_db->getString("DBLinkage"));
    db::NodeType* srcType = sourceNode->getType();
    db::NodeType* tgtType = targetNode->getType();

    if (!edgeType) {
        edgeType = _wb.createEdgeType(
            _db->getString("DBLinkage"),
            srcType,
            tgtType);
    } else {
        // Make sure that the EdgeType supports the
        // src/tgt NodeType combo
        _wb.addSourceNodeType(edgeType, srcType);
        _wb.addTargetNodeType(edgeType, tgtType);
        // addXNodeType is a tryAdd so no need to check if
        // the edgeType has the X NodeType beforehand
    }

    // Create edge
    Edge* edge = _wb.createEdge(edgeType, sourceNode, targetNode);
    if (!edge) {
        spdlog::error("Failed to create edge at line {}", _lexer.getLine());
        return false;
    }

    for (const auto& prop : _edgeProperties) {
        if (prop.first != "id") {
            const StringRef propName = _db->getString(std::string(prop.first));
            PropertyType* pt = _wb.addPropertyType(edgeType, propName, ValueType::VK_STRING);
            if (!pt) {
                pt = edgeType->getPropertyType(propName);
            }
            _wb.setProperty(edge, Property(pt, Value::createString(std::string(prop.second))));
        }
    }

    _insideEdge = false;

    return true;
}

bool GMLImport::parseGraphCommand() {
    _lexer.nextToken();

    if (_lexer.getToken().getType() != GMLToken::TK_OSBRACK) {
        spdlog::error("Unexpected token '{}' at line {}",
                      _lexer.getToken().getData(),
                      _lexer.getLine());
        return false;
    }

    if (!parseList()) {
        return false;
    }

    return true;
}

bool GMLImport::parseGenericCommand(std::string_view keyword) {
    _lexer.nextToken();
    switch (_lexer.getToken().getType()) {
        case GMLToken::TK_STRING:
        case GMLToken::TK_INT:
        case GMLToken::TK_DOUBLE:
            return handleCommand(keyword, _lexer.getToken().getData());
            break;

        case GMLToken::TK_OSBRACK:
            return parseList();
            break;

        default:
            spdlog::error("Unexpected token '{}' at line {}",
                          _lexer.getToken().getData(),
                          _lexer.getLine());
            return false;
            break;
    }

    return true;
}

bool GMLImport::handleCommand(std::string_view keyword,
                              std::string_view data) {
    if (_insideNode) {
        _nodeProperties.emplace_back(keyword, data);
    } else if (_insideEdge) {
        if (keyword == "source") {
            _source = data;
        } else if (keyword == "target") {
            _target = data;
        } else {
            _edgeProperties.emplace_back(keyword, data);
        }
    }

    return true;
}

bool GMLImport::parseList() {
    if (_lexer.getToken().getType() != GMLToken::TK_OSBRACK) {
        spdlog::error("Unexpected token '{}' at line {}",
                      _lexer.getToken().getData(),
                      _lexer.getLine());
        return false;
    }

    _lexer.nextToken();
    const auto& token = _lexer.getToken();
    while (!token.isEnd() && !token.isError() && !token.isCSBRACK()) {
        if (!parseCommand()) {
            return false;
        }
        _lexer.nextToken();
    }

    if (!token.isCSBRACK()) {
        spdlog::error("Unexpected token '{}' at line {}",
                      _lexer.getToken().getData(),
                      _lexer.getLine());
        return false;
    }

    return true;
}

Node* GMLImport::getNodeFromID(size_t id) const {
    const auto nodeIt = _nodeIDs.find(id);
    if (nodeIt == _nodeIDs.end()) {
        return nullptr;
    }

    return nodeIt->second;
}
