#include "CSVImport.h"

#include <spdlog/spdlog.h>

#include "BioAssert.h"
#include "DB.h"
#include "Edge.h"
#include "EdgeType.h"
#include "Node.h"
#include "NodeType.h"
#include "StringBuffer.h"

CSVImport::CSVImport(CSVImportConfig&& args)
    : _lexer(args.buffer->getData(), args.delimiter),
      _buffer(args.buffer),
      _db(args.db),
      _net(args.outNet),
      _wb(args.db),
      _primaryColumn(std::move(args.primaryColumn))
{
}

CSVImport::~CSVImport() {
}

bool CSVImport::run() {
    const auto& token = _lexer.getToken();
    _lexer.next();

    db::NodeType* primaryNodeType = nullptr;
    db::Node* currentPrimaryNode = nullptr;
    std::vector<db::NodeType*> nodeTypes;
    std::unordered_map<const db::NodeType*, db::EdgeType*> edgeTypes;
    const db::StringRef displayNameString = _db->getString("displayName");

    // Parsing header
    size_t col = 0;

    while (token.type != CSVLexer::Token::Type::END
           && token.type != CSVLexer::Token::Type::ERROR
           && token.type != CSVLexer::Token::Type::LINE_BREAK) {

        if (token.type == CSVLexer::Token::Type::STRING) {
            db::NodeType* nt = _wb.createNodeType(_db->getString(std::string(token.data)));
            if (!nt) {
                // sth went wrong with nt creation -> probably already exists;
                spdlog::error("Duplicate in CSV header {}", token.data);
                return false;
            }

            if ((col == 0 && _primaryColumn.empty()) || _primaryColumn == token.data) {
                _primaryColumn = token.data;
                primaryNodeType = nt;
            }

            nodeTypes.push_back(nt);
        }

        _lexer.moveRight();
        _lexer.next();
    }

    if (!primaryNodeType) {
        // Invalid primary node type
        spdlog::error("Specified primary column {} is invalid", _primaryColumn);
        return false;
    }

    const std::string& primaryNodeTypeName = primaryNodeType->getName().getSharedString()->getString();

    // Creating edge types
    for (db::NodeType* nt : nodeTypes) {
        if (nt == primaryNodeType) {
            continue;
        }

        const db::StringRef ntName = nt->getName();
        const db::StringRef etName = _db->getString(primaryNodeTypeName + "->" + ntName);
        db::EdgeType* et = _wb.createEdgeType(etName, primaryNodeType, nt);
        msgbioassert(et, "Something went wrong when creating edge type");

        edgeTypes[nt] = et;
    }

    // Parsing rows
    std::vector<db::Node*> currentNodes;
    currentNodes.reserve(nodeTypes.size());

    // Store nodes based on their name
    // (for CSV imports, nodes have a unique name which is not generally the case
    // in the DB engine)
    std::unordered_map<db::StringRef, db::Node*> nodes;

    size_t line = 1;
    while (token.type != CSVLexer::Token::Type::END
           && token.type != CSVLexer::Token::Type::ERROR) {

        currentPrimaryNode = nullptr;
        currentNodes.clear();

        // Parsing single row to create nodes
        size_t i = 0;
        size_t delimiterParsed = 0;

        while (token.type != CSVLexer::Token::Type::END
               && token.type != CSVLexer::Token::Type::ERROR
               && token.type != CSVLexer::Token::Type::LINE_BREAK) {

            if (token.type == CSVLexer::Token::Type::DELIMITER) {
                delimiterParsed++;
            }

            if (token.type == CSVLexer::Token::Type::STRING) {
                if (i >= nodeTypes.size()) {
                    spdlog::error("Too many entries at line {}", line);
                    return false;
                }

                db::NodeType* nt = nodeTypes[i];
                const db::StringRef nName = _db->getString(std::string(token.data) + " " + nt->getName());
                const db::StringRef nDisplayName = _db->getString(std::string(token.data));
                db::Node* n = nodes.find(nName) != nodes.end()
                                ? nodes.at(nName)
                                : _wb.createNode(_net, nt, nDisplayName);

                msgbioassert(n, "Something went wrong when creating node");

                if (nt == primaryNodeType) {
                    currentPrimaryNode = n;
                }

                db::PropertyType* pt = nt->getPropertyType(displayNameString);
                if (!pt) {
                    pt = _wb.addPropertyType(nt, displayNameString, db::ValueType::stringType());
                }
                db::Property p {pt, db::Value::createString(std::string {token.data})};
                _wb.setProperty(n, p);

                nodes[nName] = n;
                currentNodes.push_back(n);
                i++;
            }

            _lexer.moveRight();
            _lexer.next();
        }

        // if parsed at least one delimiter (parsed at least one node)
        // and did not parse enough nodes
        if (delimiterParsed != 0 && delimiterParsed + 1 != nodeTypes.size()) {
            spdlog::error("Missing CSV entry at line {}", line);
            return false;
        }

        // Now creating the edges
        for (db::Node* n : currentNodes) {
            if (n == currentPrimaryNode) {
                continue;
            }

            const db::NodeType* nt = n->getType();
            [[maybe_unused]] const db::Edge* e = _wb.createEdge(edgeTypes.at(nt), currentPrimaryNode, n);
            msgbioassert(e, "Something went wrong when creating edge");
        }

        _lexer.moveRight();
        _lexer.next();
        line++;
    }

    return true;
}
