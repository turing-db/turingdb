// Copyright 2023 Turing Biosystems Ltd.

#ifndef _DB_EDGE_
#define _DB_EDGE_

namespace db {

class Node;
class EdgeType;
class DB;

class Edge {
public:
    friend DB;

    EdgeType* getType() const { return _type; }

    Node* getSource() const { return _source; }
    Node* getTarget() const { return _target; }

    static Edge* create(DB* db, EdgeType* type, Node* source, Node* target);

private:
    EdgeType* _type {nullptr};
    Node* _source {nullptr};
    Node* _target {nullptr};

    Edge(EdgeType* type, Node* source, Node* target);
    ~Edge();
};

}

#endif
