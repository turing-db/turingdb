#pragma once

#include <vector>
#include <utility>
#include <string_view>
#include <unordered_map>

#include "Writeback.h"
#include "GMLLexer.h"

class StringBuffer;

class GMLImport {
public:
    GMLImport(const StringBuffer* buffer,
              db::DB* db,
              db::Network* outNet);
    ~GMLImport();

    bool run();

private:
    GMLLexer _lexer;
    db::DB* _db {nullptr};
    db::Writeback _wb;
    db::Network* _outNet {nullptr};
    db::NodeType* _nodeType {nullptr};
    db::EdgeType* _edgeType {nullptr};
    bool _insideNode {false};
    bool _insideEdge {false};
    std::string_view _source;
    std::string_view _target;
    std::vector<std::pair<std::string_view, std::string_view>> _nodeProperties;
    std::unordered_map<size_t, db::Node*> _nodeIDs;

    bool parseCommand();
    bool handleCommand(std::string_view keyword, std::string_view data);
    bool parseList();
    bool parseGraphCommand();
    bool parseGenericCommand(std::string_view keyword);
    bool parseNodeCommand();
    bool parseEdgeCommand();
    db::Node* getNodeFromID(size_t id) const;
};
