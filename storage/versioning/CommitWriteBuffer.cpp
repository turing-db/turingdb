#include "CommitWriteBuffer.h"

using namespace db;

void CommitWriteBuffer::addPendingNode(std::vector<std::string>& labels,
                                       std::vector<UntypedProperty>& properties) {
    _pendingNodes.emplace_back(labels, properties);
}

void CommitWriteBuffer::addPendingEdge(ContingentNode src, ContingentNode tgt,
                                       std::string& edgeType,
                                       std::vector<UntypedProperty>& edgeProperties) {
    _pendingEdges.emplace_back(src, tgt, edgeType, edgeProperties);
}

void CommitWriteBuffer::addDeletedNodes(const std::vector<NodeID>& newDeletedNodes) {
    _deletedNodes.insert(newDeletedNodes.begin(), newDeletedNodes.end());
}
