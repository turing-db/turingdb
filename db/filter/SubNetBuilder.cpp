#include "SubNetBuilder.h"

#include "Node.h"
#include "Edge.h"

#include "BioAssert.h"

using namespace db;

SubNetBuilder::SubNetBuilder(DB* db, Network* subNet)
    : _subNet(subNet),
    _wb(db)
{
}

SubNetBuilder::~SubNetBuilder() {
}

void SubNetBuilder::addNode(Node* node) {
    Node* clone = _wb.createNode(_subNet, node->getType());
    _subNetNodeMap[node] = clone;

    cloneProperties(node, clone);

    for (const Edge* edge : node->inEdges()) {
        const Node* nextNode = edge->getSource();
        Node* nextInSubNet = getNodeInSubNet(nextNode);
        if (nextInSubNet) {
            Edge* cloneEdge = _wb.createEdge(edge->getType(), nextInSubNet, clone);
            bioassert(cloneEdge);
            cloneProperties(edge, cloneEdge);
        }
    }

    for (const Edge* edge : node->outEdges()) {
        const Node* nextNode = edge->getTarget();
        Node* nextInSubNet = getNodeInSubNet(nextNode);
        if (nextInSubNet) {
            Edge* cloneEdge = _wb.createEdge(edge->getType(), clone, nextInSubNet);
            bioassert(cloneEdge);
            cloneProperties(edge, cloneEdge);
        }
    }
}

void SubNetBuilder::cloneProperties(const DBEntity* src, DBEntity* dest) {
    for (const auto& [propType, value] : src->properties()) {
        _wb.setProperty(dest, Property(propType, value));
    }
}

Node* SubNetBuilder::getNodeInSubNet(const Node* node) const {
    const auto it = _subNetNodeMap.find(node);
    if (it == _subNetNodeMap.end()) {
        return nullptr;
    }

    return it->second;
}
