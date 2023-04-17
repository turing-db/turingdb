#include "NodeDescriptor.h"

using namespace db;

NodeDescriptor::NodeDescriptor(NodeType* type)
    : _type(type)
{
}

NodeDescriptor::NodeDescriptor(NodeType* type, NodeDescriptor* parent)
    : _type(type),
    _parent(parent)
{
}
