#pragma once

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "DataPart.h"
#include "ID.h"
#include "writers/DataPartBuilder.h"
#include "metadata/PropertyType.h"

namespace {

using namespace db;
struct BuildNodeProperty {
    BuildNodeProperty(DataPartBuilder& builder, NodeID nid, PropertyTypeID pid)
        : _builder(builder),
          _nid(nid),
          _pid(pid) {}

    void operator()(types::Int64::Primitive propValue) const {
        _builder.addNodeProperty<types::Int64>(_nid, _pid, propValue);
    }
    void operator()(types::UInt64::Primitive propValue) const {
        _builder.addNodeProperty<types::UInt64>(_nid, _pid, propValue);
    }
    void operator()(types::Double::Primitive propValue) const {
        _builder.addNodeProperty<types::Double>(_nid, _pid, propValue);
    }
    void operator()(types::String::Primitive propValue) const {
        _builder.addNodeProperty<types::String>(_nid, _pid, propValue);
    }
    void operator()(types::Bool::Primitive propValue) const {
        _builder.addNodeProperty<types::Bool>(_nid, _pid, propValue);
    }


private:
    DataPartBuilder& _builder;
    NodeID _nid;
    PropertyTypeID _pid;
};
} // Anonymous namespace

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

     // A node: either exists in previous commit (materialised as NodeID),
     // or to be created in this commit (materialised as PendingNode)
     using ContingentNode = std::variant<NodeID, PendingNode>;
 private:
     struct PendingEdge {
         ContingentNode src;
         ContingentNode tgt;
         std::vector<std::string> edgeLabelTypeNames;
         std::vector<UntypedProperty> properties;
    };

public:
    CommitWriteBuffer() = default;

    void addPendingNode(std::vector<std::string>& labels,
                        std::vector<UntypedProperty>& properties) {
        _pendingNodes.emplace_back(labels, properties);
    }

    void addPendingEdge(ContingentNode src, ContingentNode tgt,
                        std::vector<std::string> edgeLabels,
                        std::vector<UntypedProperty> edgeProperties) {
        _pendingEdges.emplace_back();
    }

    /**
     * @brief Adds NodeIDs contained in @param newDeletedNodes to the member @ref
     * _deletedNodes
     * @detail Calls std::vector::reserve for the additional space before calling
     * std::vector::insert
     */
    void addDeletedNodes(const std::vector<NodeID>& newDeletedNodes) {
        _deletedNodes.reserve(_deletedNodes.size() + newDeletedNodes.size());
        _deletedNodes.insert(_deletedNodes.end(),
                             newDeletedNodes.begin(),
                             newDeletedNodes.end());
    }
    
private:

    // Nodes to be created
    std::vector<PendingNode> _pendingNodes;

    // Edges to be created between two nodes that are created in this commit
    std::vector<PendingEdge> _pendingEdges;
    
    // Edges to be created whose source exists in a previous commit, but whose target is created in this commit
     
    // Edges to be created whose target exists in a previous commit, but whose source is created in this commit
    
    // Edges to be created whose source and target exist in a previous commit

    // Nodes to be deleted
    std::vector<NodeID> _deletedNodes;

    // Edges to be deleted
    std::vector<EdgeID> _deletedEdges;
};

}
