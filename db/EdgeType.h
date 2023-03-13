// Copyright 2023 Turing Biosystems Ltd.

#ifndef _DB_EDGE_TYPE_
#define _DB_EDGE_TYPE_

#include <vector>

#include "StringRef.h"

namespace db {

class DB;
class NodeType;

class EdgeType {
public:
    friend DB;

    StringRef getName() const { return _name; }

    NodeType* getSourceType() const { return _sourceType; }
    NodeType* getTargetType() const { return _targetType; }

    static EdgeType* create(DB* db,
                            StringRef name,
                            NodeType* source,
                            NodeType* target);

private:
    StringRef _name;
    NodeType* _sourceType {nullptr};
    NodeType* _targetType {nullptr};

    EdgeType(StringRef name,
             NodeType* sourceType,
             NodeType* targetType);
    ~EdgeType();
};

}

#endif
