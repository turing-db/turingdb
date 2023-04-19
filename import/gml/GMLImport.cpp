#include "GMLImport.h"

#include "DB.h"
#include "NodeType.h"
#include "Writeback.h"

#include "StringBuffer.h"
#include "StringToNumber.h"

using namespace db;

GMLImport::GMLImport(const StringBuffer* buffer,
                     db::DB* db,
                     db::Network* outNet)
    : _lexer(buffer->getData(), buffer->getSize()),
    _wb(db),
    _outNet(outNet)
{
    _nodeType = _wb.createNodeType(db->getString("Generic"));
    _edgeType = _wb.createEdgeType(db->getString("Edge"),
                                   _nodeType->getBaseComponent(),
                                   _nodeType->getBaseComponent());
}

GMLImport::~GMLImport() {
}

bool GMLImport::run() {
    if (!_nodeType) {
        return false;
    }

    if (!_edgeType) {
        return false;
    }

    // Start parsing file
    _lexer.nextToken();

    const auto& token = _lexer.getToken();
    while (!token.isEnd() && !token.isError()) {
        if (!parseCommand()) {
            return false;
        }
        _lexer.nextToken();
    }

    return true;
}

bool GMLImport::parseCommand() {
    if (_lexer.getToken().getType() != GMLToken::TK_STRING) {
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
        }
    }

    if (errorID) {
        return false;
    }

    Node* node = _wb.createNode(_outNet, _nodeType);
    if (!node) {
        return false;
    }

    _nodeIDs[nodeID] = node;

    _insideNode = false;

    return true;
}

bool GMLImport::parseEdgeCommand() {
    _source = std::string_view();
    _target = std::string_view();

    _insideEdge = true;

    _lexer.nextToken();

    if (_lexer.getToken().getType() != GMLToken::TK_OSBRACK) {
        return false;
    }

    if (!parseList()) {
        return false;
    }

    if (_source.empty() || _target.empty()) {
        return false;
    }

    size_t sourceID = 0;
    size_t targetID = 0;
    bool errorSource = true;
    bool errorTarget = true;
    sourceID = StringToNumber<size_t>(_source, errorSource);
    targetID = StringToNumber<size_t>(_source, errorTarget);
    
    if (errorSource || errorTarget) {
        return false;
    }

    // Get source and target nodes
    Node* sourceNode = getNodeFromID(sourceID);
    Node* targetNode = getNodeFromID(targetID);
    if (!sourceNode) {
        return false;
    }

    if (!targetNode) {
        return false;
    }

    // Create edge
    Edge* edge = _wb.createEdge(_edgeType, sourceNode, targetNode);
    if (!edge) {
        return false;
    }

    _insideEdge = false;

    return true;
}

bool GMLImport::parseGraphCommand() {
    _lexer.nextToken();

    if (_lexer.getToken().getType() != GMLToken::TK_OSBRACK) {
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
            return handleCommand(keyword, _lexer.getToken().getData());
        break;

        case GMLToken::TK_OSBRACK:
            return parseList();
        break;

        default:
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
        }
    }

    return true;
}

bool GMLImport::parseList() {
    if (_lexer.getToken().getType() != GMLToken::TK_OSBRACK) {
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
