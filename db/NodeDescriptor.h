#pragma once

namespace db {

class NodeType;

class NodeDescriptor {
public:
    NodeDescriptor(NodeType* type);
    NodeDescriptor(NodeType* type, NodeDescriptor* parent);

    NodeType* getType() const { return _type; }

    NodeDescriptor* getParent() const { return _parent; }

private:
    NodeType* _type {nullptr};
    NodeDescriptor* _parent {nullptr};
};

}
