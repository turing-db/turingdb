#include "Writeback.h"

#include "DB.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "ComponentType.h"
#include "Property.h"
#include "NodeDescriptor.h"
#include "Edge.h"
#include "EdgeType.h"

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

bool Writeback::addComponent(Node* node, ComponentType* compType) {
    if (!node || !compType) {
        return false;
    }

    NodeDescriptor* nodeDesc = node->_desc;
    if (nodeDesc->hasComponent(compType)) {
        return false;
    }

    NodeDescriptor* newDesc = nodeDesc->getOrCreateChild(compType);
    node->setDescriptor(newDesc);

    newDesc->addComponent(compType);

    return true;
}

Edge* Writeback::createEdge(EdgeType* type, Node* source, Node* target) {
    if (!type || !source || !target) {
        return nullptr;
    }

    Edge* edge = new Edge(type, source, target);
    source->addOutEdge(edge);
    target->addInEdge(edge);
    return edge;
}

NodeType* Writeback::createNodeType(StringRef name) {
    if (_db->getNodeType(name)) {
        return nullptr;
    }

    // Create base component
    ComponentType* baseComp = createComponentType(name);
    if (!baseComp) {
        return nullptr;
    }

    // Create node type
    NodeType* nodeType = new NodeType(name);
    _db->addNodeType(nodeType);

    // Add base component
    NodeDescriptor* rootDesc = nodeType->_rootDesc;
    rootDesc->setBaseComponent(baseComp);
    rootDesc->addComponent(baseComp);

    return nodeType;
}

EdgeType* Writeback::createEdgeType(StringRef name,
                                    ComponentType* sourceComp,
                                    ComponentType* targetComp) {
    if (!sourceComp || !targetComp) {
        return nullptr;
    }

    if (_db->getEdgeType(name)) {
        return nullptr;
    }

    EdgeType* edgeType = new EdgeType(name, sourceComp, targetComp);
    _db->addEdgeType(edgeType);
    sourceComp->addEdgeType(edgeType);
    targetComp->addEdgeType(edgeType);
    return edgeType;
}

ComponentType* Writeback::createComponentType(StringRef name) {
    if (_db->getComponentType(name)) {
        return nullptr;
    }

    ComponentType* compType = new ComponentType(name);
    _db->addComponentType(compType);

    return compType;
}

Property* Writeback::addProperty(ComponentType* compType,
                                 StringRef name,
                                 ValueType* valType) {
    if (!compType || !valType) {
        return nullptr;
    }

    if (compType->getProperty(name)) {
        return nullptr;
    }

    Property* prop = new Property(name, valType, compType);
    compType->addProperty(prop);

    return prop;
}
