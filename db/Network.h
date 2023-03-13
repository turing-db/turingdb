// Copyright 2023 Turing Biosystems Ltd.

#ifndef _DB_NETWORK_
#define _DB_NETWORK_

#include <vector>

#include "StringRef.h"

namespace db {

class DB;
class Node;
class Edge;

class Network {
public:
    friend DB;
    friend Node;
    friend Edge;
    using Networks = std::vector<Network*>;
    using Nodes = std::vector<Node*>;

    StringRef getDisplayName() const { return _displayName; }

    Network* getParent() const { return _parent; }

    const Networks& subNetworks() const { return _subNets; }

    const Nodes& nodes() const { return _nodes; }

    static Network* create(DB* db, StringRef displayName);
    static Network* create(DB* db, Network* parent, StringRef displayName);

private:
    StringRef _displayName;
    Network* _parent {nullptr};
    Networks _subNets;
    Nodes _nodes;

    Network(StringRef displayName);
    ~Network();
    void setParent(Network* parent);
    void addSubNet(Network* subNet);
    void addNode(Node* node);
};

}

#endif
