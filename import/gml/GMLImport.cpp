#include "GMLImport.h"

#include "DB.h"
#include "NodeType.h"
#include "EdgeType.h"
#include "Node.h"
#include "Edge.h"
#include "Property.h"
#include "Writeback.h"

#include "StringBuffer.h"
#include "StringToNumber.h"

#include "BioLog.h"
#include "MsgImport.h"

using namespace db;
using namespace Log;

GMLImport::GMLImport(const StringBuffer* buffer,
                     db::DB* db,
                     db::Network* outNet)
    : _lexer(buffer->getData(), buffer->getSize()),
    _db(db),
    _wb(db),
    _outNet(outNet)
{
    _nodeType = _wb.createNodeType(db->getString("GenericNode"));
    _edgeType = _wb.createEdgeType(db->getString("GenericEdge"), _nodeType, _nodeType);
}

GMLImport::~GMLImport() {
}

bool GMLImport::run() {
    if (!_nodeType) {
        BioLog::log(msg::ERROR_NODE_TYPE_CREATE_FAILED());
        return false;
    }

    if (!_edgeType) {
        BioLog::log(msg::ERROR_EDGE_TYPE_CREATE_FAILED());
        return false;
    }

    // Start parsing file
    _lexer.nextToken();

    const auto& token = _lexer.getToken();
    while (!token.isEnd() && !token.isError()) {
        if (!parseCommand()) {
            BioLog::log(msg::ERROR_FAILED_PARSE_GML_COMMAND()
                        << _lexer.getLine());
            return false;
        }
        _lexer.nextToken();
    }

    return true;
}

bool GMLImport::parseCommand() {
    if (_lexer.getToken().getType() != GMLToken::TK_STRING) {
        BioLog::log(msg::ERROR_UNEXPECTED_TOKEN()
                    << std::string(_lexer.getToken().getData())
                    << _lexer.getLine());
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
        BioLog::log(msg::ERROR_UNEXPECTED_TOKEN()
                    << std::string(_lexer.getToken().getData())
                    << _lexer.getLine());
        return false;
    }

    if (!parseList()) {
        return false;
    }

    // Get node ID
    // It must be among the properties of the node
    size_t nodeID = 0;
    bool errorID = true;
    for (const auto& prop : _nodeProperties) {
        if (prop.first == "id") {
            nodeID = StringToNumber<size_t>(prop.second, errorID);
            break;
        }
    }

    if (errorID) {
        BioLog::log(msg::ERROR_NODE_ID_NOT_FOUND()
                    << _lexer.getLine());
        return false;
    }

    Node* node = _wb.createNode(_outNet, _nodeType);
    if (!node) {
        BioLog::log(msg::ERROR_FAILED_TO_CREATE_NODE() << _lexer.getLine());
        return false;
    }

    for (const auto& prop : _nodeProperties) {
        if (prop.first != "id") {
            const StringRef propName = _db->getString(std::string(prop.first));
            PropertyType* pt = _wb.addPropertyType(_nodeType, propName, ValueType::VK_STRING);
            if (!pt) {
                pt = _nodeType->getPropertyType(propName);
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
        BioLog::log(msg::ERROR_UNEXPECTED_TOKEN()
                    << std::string(_lexer.getToken().getData())
                    << _lexer.getLine());
        return false;
    }

    if (!parseList()) {
        return false;
    }

    if (_source.empty() || _target.empty()) {
        BioLog::log(msg::ERROR_NO_SOURCE_OR_NO_EDGE()
                    << _lexer.getLine());
        return false;
    }

    size_t sourceID = 0;
    size_t targetID = 0;
    bool errorSource = true;
    bool errorTarget = true;
    sourceID = StringToNumber<size_t>(_source, errorSource);
    targetID = StringToNumber<size_t>(_target, errorTarget);
    
    if (errorSource) {
        BioLog::log(msg::ERROR_IMPOSSIBLE_TO_CONVERT_ID()
                    << std::string(_source) << _lexer.getLine());
        return false;
    }

    if (errorTarget) {
        BioLog::log(msg::ERROR_IMPOSSIBLE_TO_CONVERT_ID()
                    << std::string(_target) << _lexer.getLine());
        return false;
    }

    // Get source and target nodes
    Node* sourceNode = getNodeFromID(sourceID);
    Node* targetNode = getNodeFromID(targetID);
    if (!sourceNode) {
        BioLog::log(msg::ERROR_NODE_ID_NOT_FOUND()
                    << sourceID << _lexer.getLine());
        return false;
    }

    if (!targetNode) {
        BioLog::log(msg::ERROR_NODE_ID_NOT_FOUND()
                    << targetID << _lexer.getLine());
        return false;
    }

    // Create edge
    Edge* edge = _wb.createEdge(_edgeType, sourceNode, targetNode);
    if (!edge) {
        BioLog::log(msg::ERROR_FAILED_TO_CREATE_EDGE() << _lexer.getLine());
        return false;
    }

    for (const auto& prop : _edgeProperties) {
        if (prop.first != "id") {
            const StringRef propName = _db->getString(std::string(prop.first));
            PropertyType* pt = _wb.addPropertyType(_edgeType, propName, ValueType::VK_STRING);
            if (!pt) {
                pt = _edgeType->getPropertyType(propName);
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
        BioLog::log(msg::ERROR_UNEXPECTED_TOKEN()
                    << std::string(_lexer.getToken().getData())
                    << _lexer.getLine());
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
            BioLog::log(msg::ERROR_UNEXPECTED_TOKEN()
                        << std::string(_lexer.getToken().getData()) 
                        << _lexer.getLine());
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
        BioLog::log(msg::ERROR_UNEXPECTED_TOKEN()
                    << std::string(_lexer.getToken().getData())
                    << _lexer.getLine());
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
        BioLog::log(msg::ERROR_UNEXPECTED_TOKEN()
                    << std::string(token.getData())
                    << _lexer.getLine());
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
