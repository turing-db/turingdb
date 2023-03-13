// Copyright 2023 Turing Biosystems Ltd.

#ifndef _DB_NODE_
#define _DB_NODE_

namespace db {

class Network;
class NodeType;
class Edge;

class Node {
public:
    friend Network;
    friend Edge;

    NodeType* getType() const { return _type; }

    Network* getNetwork() const { return _net; }

    static Node* create(Network* net, NodeType* type);

private:
    NodeType* _type {nullptr};
    Network* _net {nullptr};

    Node(Network* net, NodeType* type);
    ~Node();
    void addInEdge(Edge* edge);
    void addOutEdge(Edge* edge);
};

}

#endif
