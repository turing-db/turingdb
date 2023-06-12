// Copyright 2023 Turing Biosystems Ltd.

#include "EdgeType.h"

#include "NodeType.h"

using namespace db;

EdgeType::EdgeType(DBIndex index,
                   StringRef name,
                   const std::vector<NodeType*>& sources,
                   const std::vector<NodeType*>& targets)
    : DBEntityType(index, name),
    _sourceTypes(sources.begin(), sources.end()),
    _targetTypes(targets.begin(), targets.end())
{
}

EdgeType::~EdgeType() {
}

bool EdgeType::hasSourceType(const NodeType* nodeType) const {
    return (_sourceTypes.find(const_cast<NodeType*>(nodeType)) != _sourceTypes.end());
}

bool EdgeType::hasTargetType(const NodeType* nodeType) const {
    return (_targetTypes.find(const_cast<NodeType*>(nodeType)) != _targetTypes.end());
}

EdgeType::NodeTypeRange EdgeType::sourceTypes() const {
    return NodeTypeRange(&_sourceTypes);
}

EdgeType::NodeTypeRange EdgeType::targetTypes() const {
    return NodeTypeRange(&_targetTypes);
}

void EdgeType::addSourceType(NodeType* nodeType) {
    _sourceTypes.insert(nodeType);
}

void EdgeType::addTargetType(NodeType* nodeType) {
    _targetTypes.insert(nodeType);
}
