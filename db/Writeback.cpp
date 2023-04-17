#include "Writeback.h"

#include "DB.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "ComponentType.h"

using namespace db;

Writeback::Writeback(DB* db)
    : _db(db)
{
}

Writeback::~Writeback() {
}

Network* Writeback::createNetwork(StringRef name) {
    if (_db->getNetwork(name)) {
        return nullptr;
    }

    Network* net = new Network(name);
    _db->addNetwork(net);
    return net;
}

Node* Writeback::createNode(Network* net, NodeType* type) {
    if (!net || !type) {
        return nullptr;
    }

    Node* node = new Node(type->_rootDesc);
    net->addNode(node);

    return node;
}

NodeType* Writeback::createNodeType(StringRef name) {
    if (_db->getNodeType(name)) {
        return nullptr;
    }

    NodeType* nodeType = new NodeType(name);
    _db->addNodeType(nodeType);

    return nodeType;
}

ComponentType* Writeback::createComponentType(StringRef name) {
    if (_db->getComponentType(name)) {
        return nullptr;
    }

    ComponentType* compType = new ComponentType(name);
    _db->addComponentType(compType);

    return compType;
}
