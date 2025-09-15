#pragma once

#include <set>
#include <string>
#include <variant>
#include <vector>

#include "DataPart.h"
#include "ID.h"
#include "metadata/PropertyType.h"

namespace {

using namespace db;

} 

namespace db {


class CommitWriteBuffer {
public:
    using SupportedTypeVariant =
        std::variant<types::Int64::Primitive, types::UInt64::Primitive,
                     types::Double::Primitive, types::String::Primitive,
                     types::Bool::Primitive>;

     struct UntypedProperty {
        std::string propertyName;
        SupportedTypeVariant value;
     };

     struct PendingNode {
         std::vector<std::string> labelNames;
         std::vector<UntypedProperty> properties;
     };

     using PendingNodeOffset = size_t;

     // A node: either exists in previous commit (materialised as NodeID),
     // or to be created in this commit (materialised as PendingNode)
     using ContingentNode = std::variant<NodeID, PendingNodeOffset>;
 private:
     struct PendingEdge {
         ContingentNode src;
         ContingentNode tgt;
         std::vector<std::string> edgeLabelTypeNames;
         std::vector<UntypedProperty> properties;
    };

public:
    CommitWriteBuffer() = default;

    PendingNodeOffset nextPendingNodeOffset() { return _pendingNodes.size(); }

    void addPendingNode(std::vector<std::string>& labels,
                        std::vector<UntypedProperty>& properties) {
        _pendingNodes.emplace_back(labels, properties);
    }

    void addPendingEdge(ContingentNode src, ContingentNode tgt,
                        std::vector<std::string>& edgeLabels,
                        std::vector<UntypedProperty>& edgeProperties) {
        _pendingEdges.emplace_back(src, tgt, edgeLabels, edgeProperties);
    }

    std::vector<PendingNode>& pendingNodes() { return _pendingNodes; }
    std::vector<PendingEdge>& pendingEdges() { return _pendingEdges; }

    const std::set<NodeID> deletedNodes() const { return _deletedNodes; }

    /**
     * @brief Adds NodeIDs contained in @param newDeletedNodes to the member @ref
     * _deletedNodes
     * @detail Calls std::vector::reserve for the additional space before calling
     * std::vector::insert
     */
    void addDeletedNodes(const std::vector<NodeID>& newDeletedNodes) {
        // _deletedNodes.reserve(_deletedNodes.size() + newDeletedNodes.size());

        _deletedNodes.insert(newDeletedNodes.begin(), newDeletedNodes.end());
    }
    
private:
    friend DataPartBuilder;

    // Nodes to be created
    std::vector<PendingNode> _pendingNodes;

    // Edges to be created
    std::vector<PendingEdge> _pendingEdges;
    
    // Nodes to be deleted
    std::set<NodeID> _deletedNodes;

    // Edges to be deleted
    std::set<EdgeID> _deletedEdges;

    

    struct ValueTypeFromProperty {
        ValueType operator()(types::Int64::Primitive propValue) const {
            return types::Int64::_valueType;
        }
        ValueType operator()(types::UInt64::Primitive propValue) const {
            return types::UInt64::_valueType;
        }
        ValueType operator()(types::Double::Primitive propValue) const {
            return types::Double::_valueType;
        }
        ValueType operator()(types::String::Primitive propValue) const {
            return types::String::_valueType;
        }
        ValueType operator()(types::Bool::Primitive propValue) const {
            return types::Bool::_valueType;
        }
    };
};

}
