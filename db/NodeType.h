// Copyright 2023 Turing Biosystems Ltd.

#ifndef _DB_NODE_TYPE_
#define _DB_NODE_TYPE_

#include <vector>

#include "StringRef.h"

namespace db {

class DB;
class EdgeType;

class NodeType {
public:
    friend DB;
    friend EdgeType;
    using EdgeTypes = std::vector<EdgeType*>;

    StringRef getName() const { return _name; }

    const EdgeTypes& getInLinkTypes() const { return _inEdgeTypes; }
    const EdgeTypes& getOutLinkTypes() const { return _outEdgeTypes; }

    static NodeType* create(DB* db, StringRef name);

private:
    StringRef _name;
    EdgeTypes _inEdgeTypes;
    EdgeTypes _outEdgeTypes;

    NodeType(StringRef name);
    ~NodeType();
    void addInEdgeType(EdgeType* edgeType);
    void addOutEdgeType(EdgeType* edgeType);
};

}

#endif
