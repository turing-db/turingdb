#include "WriteStep.h"

#include <limits>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/chunk.hpp>

#include "Expr.h"
#include "ID.h"
#include "InjectedIDs.h"
#include "PathPattern.h"
#include "PipelineException.h"
#include "Profiler.h"
#include "SystemManager.h"
#include "TypeConstraint.h"
#include "metadata/LabelSet.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "versioning/CommitBuilder.h"
#include "writers/MetadataBuilder.h"

namespace rg = ranges;
namespace rv = rg::views;

using namespace db;

namespace {

void addUntypedProperties(CommitWriteBuffer::UntypedProperties& props,
                          const BinExpr* propExpr,
                          MetadataBuilder& metadataBuilder) {
    const VarExpr* left = static_cast<const VarExpr*>(propExpr->getLeftExpr());
    const ExprConst* right = static_cast<const ExprConst*>(propExpr->getRightExpr());

    if (left->getKind() != Expr::EK_VAR_EXPR) {
        throw PipelineException("Node property expression must be an assignment");
    }

    const std::string& propertyName = left->getName();

    const ValueType valueType = right->getType();
    switch (valueType) {
        case ValueType::Int64: {
            const auto* casted = static_cast<const Int64ExprConst*>(right);
            PropertyTypeID propID =
                metadataBuilder.getOrCreatePropertyType(propertyName, casted->getType())
                    ._id;

            props.emplace_back(propID, casted->getVal());
            break;
        }
        case ValueType::UInt64: {
            const auto* casted = static_cast<const UInt64ExprConst*>(right);
            PropertyTypeID propID =
                metadataBuilder.getOrCreatePropertyType(propertyName, casted->getType())
                    ._id;

            props.emplace_back(propID, casted->getVal());
            break;
        }
        case ValueType::Double: {
            const auto* casted = static_cast<const DoubleExprConst*>(right);
            PropertyTypeID propID =
                metadataBuilder.getOrCreatePropertyType(propertyName, casted->getType())
                    ._id;

            props.emplace_back(propID, casted->getVal());
            break;
        }
        case ValueType::String: {
            const auto* casted = static_cast<const StringExprConst*>(right);
            PropertyTypeID propID =
                metadataBuilder.getOrCreatePropertyType(propertyName, casted->getType())
                    ._id;

            props.emplace_back(propID, casted->getVal());
            break;
        }
        case ValueType::Bool: {
            const auto* casted = static_cast<const BoolExprConst*>(right);
           PropertyTypeID propID =
                metadataBuilder.getOrCreatePropertyType(propertyName, casted->getType())
                    ._id;

           props.emplace_back(propID, casted->getVal());
           break;
        }
        default: {
            throw PipelineException("Unsupported value type");
        }
    }
}
}

void WriteStep::prepare(ExecutionContext* ctxt) {
    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx->writingPendingCommit()) {
        throw PipelineException("WriteStep: Cannot perform writes outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();
    CommitBuilder* commitBuilder = tx.commitBuilder();

    _metadataBuilder = &commitBuilder->metadata();
    _writeBuffer = &commitBuilder->writeBuffer();
}

CommitWriteBuffer::PendingNodeOffset WriteStep::writeNode(const EntityPattern* nodePattern) {
    const TypeConstraint* patternLabels = nodePattern->getTypeConstraint();
    LabelSet labelset;
    if (patternLabels != nullptr) {
        for (const VarExpr* name : patternLabels->getTypeNames()) {
            labelset.set(_metadataBuilder->getOrCreateLabel(name->getName()));
        }
    } else {
        throw PipelineException("Nodes must have at least one label");
    }

    // NOTE: Only create a PendingNode after we pass the above check, and no exception
    // thrown

    // Build a @ref CommitWriteBuffer::PendingNode with empty fields
    CommitWriteBuffer::PendingNode& newNode = _writeBuffer->newPendingNode();

    newNode.labelsetHandle = _metadataBuilder->getOrCreateLabelSet(labelset);

    // Set the properties of the PendingNode
    const ExprConstraint* patternProperties = nodePattern->getExprConstraint();
    if (patternProperties != nullptr) {
        for (const BinExpr* expr : patternProperties->getExpressions()) {
            addUntypedProperties(newNode.properties, expr, *_metadataBuilder);
        }
    }

    // Record this node's offset for later reference
    CommitWriteBuffer::PendingNodeOffset thisNodeOffset =
        _writeBuffer->numPendingNodes() - 1;
    const VarDecl* nodeVarDecl = nodePattern->getVar()->getDecl();

    _varOffsetMap[nodeVarDecl] = thisNodeOffset;

    return thisNodeOffset;
}

CommitWriteBuffer::ExistingOrPendingNode WriteStep::getOrWriteNode(const EntityPattern* nodePattern) {
    const VarDecl* nodeVarDecl = nodePattern->getVar()->getDecl();

    // Check to see if this node has been written already by searching its variable name
    auto pendingNodeIt = _varOffsetMap.find(nodeVarDecl);
    if (pendingNodeIt != _varOffsetMap.end()) {
        return pendingNodeIt->second; // PendingNodeOffset to the PendingNode of this node
    }

    // Check to see if @ref nodePattern has injected ID, return that ID if so
    auto *injectedIDs = nodePattern->getInjectedIDs();
    if (injectedIDs != nullptr) {
        // @ref QueryPlanner::analyzeEntityPattern ensures there is exactly 1
        // injected ID (if any)
        return NodeID {injectedIDs->getIDs().front()};
    }

    // If this node has not been written, and it does not have a specific NodeID injected,
    // we must write it now
    CommitWriteBuffer::PendingNodeOffset newOffset = writeNode(nodePattern);
    return newOffset;
}

void WriteStep::writeEdge(const ContingentNode src, const ContingentNode tgt,
                          const EntityPattern* edgePattern) {
    // Get the EdgeType for PendingEdge
    const TypeConstraint* patternType = edgePattern->getTypeConstraint();

    EdgeTypeID pendingEdgeType;

    if (patternType != nullptr) {
        pendingEdgeType = _metadataBuilder->getOrCreateEdgeType(
            patternType->getTypeNames().front()->getName());
    } else [[unlikely]] {
        throw PipelineException("Edges must have at least one label");
    }

    // NOTE: Only create a PendingEdge after we pass the above check, and no exception
    // thrown
    auto& pendingEdge = _writeBuffer->newPendingEdge(src, tgt);
    pendingEdge.edgeType = pendingEdgeType;

    const ExprConstraint* patternProperties = edgePattern->getExprConstraint();
    if (patternProperties != nullptr) {
        for (const BinExpr* expr : patternProperties->getExpressions()) {
            addUntypedProperties(pendingEdge.properties, expr, *_metadataBuilder);
        }
    }

    pendingEdge.src = src;
    pendingEdge.tgt = tgt;
}

void WriteStep::writePath(const PathPattern* pathPattern) {
    std::span pathElements {pathPattern->elements()};

    // Process paths in tuples of (src, (edge, tgt)), allowing us to reuse src in the
    // current edge as tgt in the next edge

    // Process our 'src' in the (src, (edge, tgt)) tuple
    const EntityPattern* srcPattern = pathElements.front();
    ContingentNode srcNode = getOrWriteNode(srcPattern);

    // Drop the src of our tuple: already processed. Chunk by 2 to get (edge, tgt)
    for (auto step : pathElements | rv::drop(1) | rv::chunk(2)) {
        const EntityPattern* edgePattern = step.front();

        const EntityPattern* tgtPattern = step.back();
        ContingentNode tgtNode = getOrWriteNode(tgtPattern);

        writeEdge(srcNode, tgtNode, edgePattern);

        srcNode = tgtNode; // Assign the current target as the source for the next edge
    }
}

void WriteStep::execute() {
    Profile profile {"WriteStep::execute"};

    for (const CreateTarget* target : *_targets) {
        const PathPattern* path = target->getPattern();
        std::span pathElements {path->elements()};

        if (pathElements.size() == 1) {
            writeNode(pathElements.front());
        } else {
            writePath(path);
        }
    }
}

void WriteStep::describe(std::string& descr) const {
    descr.clear();
    descr += "WriteStep";
}
