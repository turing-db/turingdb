#include "WriteStep.h"

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
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "versioning/CommitBuilder.h"

namespace rv = ranges::views;

using namespace db;

void WriteStep::prepare(ExecutionContext* ctxt) {
    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx->writingPendingCommit()) {
        throw PipelineException("WriteStep: Cannot perform writes outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();

    _writeBuffer = &tx.commitBuilder()->writeBuffer();
}

CommitWriteBuffer::PendingNodeOffset WriteStep::writeNode(const EntityPattern* nodePattern) {
    // Build a @ref CommitWriteBuffer::PendingNode:
     
    // Labels to pass to the PendingNode
    const TypeConstraint* patternLabels = nodePattern->getTypeConstraint();
    std::vector<std::string> nodeLabels;
    if (patternLabels) {
        for (const auto& name : patternLabels->getTypeNames()) {
            nodeLabels.emplace_back(name->getName());
        }
    } else {
        throw PipelineException("Nodes must have at least one label");
    }

    // Properties to pass to the PendingNode
    UntypedProperties nodeProperties;

    const ExprConstraint* patternProperties = nodePattern->getExprConstraint();
    if (!patternProperties) { // Early exit if no properties
        // Add this node to the write buffer, and record its offset
        CommitWriteBuffer::PendingNodeOffset thisNodeOffset =
            _writeBuffer->nextPendingNodeOffset();
        const std::string& nodeVarName = nodePattern->getVar()->getName();

        _writeBuffer->addPendingNode(nodeLabels, nodeProperties);
        _varOffsetMap[nodeVarName] = thisNodeOffset;

        return thisNodeOffset;
    }

    for (const auto& e : patternProperties->getExpressions()) {
        const auto& left = static_cast<const VarExpr*>(e->getLeftExpr());
        const auto& right = static_cast<const ExprConst*>(e->getRightExpr());

        if (left->getKind() != Expr::EK_VAR_EXPR) {
            throw PipelineException("Node property expression must be an assignment");
        }

        const std::string& propertyName = left->getName();

        const ValueType valueType = right->getType();
        switch (valueType) {
            case ValueType::Int64: {
                const auto* casted = static_cast<const Int64ExprConst*>(right);
                nodeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            case ValueType::UInt64: {
                const auto* casted = static_cast<const UInt64ExprConst*>(right);
                nodeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            case ValueType::Double: {
                const auto* casted = static_cast<const DoubleExprConst*>(right);
                nodeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            case ValueType::String: {
                const auto* casted = static_cast<const StringExprConst*>(right);
                nodeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            case ValueType::Bool: {
                const auto* casted = static_cast<const BoolExprConst*>(right);
                nodeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            default: {
                throw std::runtime_error("Unsupported value type");
            }
        }
    }

    // Add this node to the write buffer, and record its offset
    CommitWriteBuffer::PendingNodeOffset thisNodeOffset =
        _writeBuffer->nextPendingNodeOffset();
    const std::string& nodeVarName = nodePattern->getVar()->getName();

    _writeBuffer->addPendingNode(nodeLabels, nodeProperties);
    _varOffsetMap[nodeVarName] = thisNodeOffset;

    return thisNodeOffset;
}

CommitWriteBuffer::ContingentNode WriteStep::getOrWriteNode(const EntityPattern* nodePattern) {
    const std::string& nodeVarName = nodePattern->getVar()->getName();

    // Check to see if this node has been written already by searching its variable name
    auto pendingNodeIt = _varOffsetMap.find(nodeVarName);

    if (pendingNodeIt != _varOffsetMap.end()) {
        return pendingNodeIt->second; // PendingNodeOffset to the PendingNode of this node
    }

    // Check to see if @ref nodePattern has injected ID, return that ID if so
    auto injectedIDs = nodePattern->getInjectedIDs();
    if (injectedIDs) {
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
    // Get the labels for PendingEdge
    const TypeConstraint* patternType = edgePattern->getTypeConstraint();
    std::string edgeType;
    if (patternType) {
            edgeType = patternType->getTypeNames().front()->getName();
    } else {
        throw PipelineException("Edges must have at least one label");
    }

    const ExprConstraint* patternProperties = edgePattern->getExprConstraint();
    if (!patternProperties) {
        UntypedProperties emptyProps;
        _writeBuffer->addPendingEdge(src, tgt, edgeType, emptyProps);
        return;
    }

    // Formulate the properties for PendingEdge
    UntypedProperties edgeProperties;

    for (const auto& e : patternProperties->getExpressions()) {
        const auto& left = static_cast<const VarExpr*>(e->getLeftExpr());
        const auto& right = static_cast<const ExprConst*>(e->getRightExpr());

        if (left->getKind() != Expr::EK_VAR_EXPR) {
            throw PipelineException("Node property expression must be an assignment");
        }

        const std::string& propertyName = left->getName();

        const ValueType valueType = right->getType();

        switch (valueType) {
            case ValueType::Int64: {
                const auto* casted = static_cast<const Int64ExprConst*>(right);
                edgeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            case ValueType::UInt64: {
                const auto* casted = static_cast<const UInt64ExprConst*>(right);
                edgeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            case ValueType::Double: {
                const auto* casted = static_cast<const DoubleExprConst*>(right);
                edgeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            case ValueType::String: {
                const auto* casted = static_cast<const StringExprConst*>(right);
                edgeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            case ValueType::Bool: {
                const auto* casted = static_cast<const BoolExprConst*>(right);
                edgeProperties.emplace_back(propertyName, casted->getVal());
                break;
            }
            default: {
                throw std::runtime_error("Unsupported value type");
            }
        }
    }
    _writeBuffer->addPendingEdge(src, tgt, edgeType, edgeProperties);
}


void WriteStep::writeEdges(const PathPattern* pathPattern) {
    std::span pathElements {pathPattern->elements()};

    const EntityPattern* srcPattern = pathElements.front();

    ContingentNode srcNode = getOrWriteNode(srcPattern);

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
            writeEdges(path);
        }
    }
}

void WriteStep::describe(std::string& descr) const {
    descr.clear();
    descr += "WriteStep";
}
