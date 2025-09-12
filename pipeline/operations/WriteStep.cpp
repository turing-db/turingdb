#include "WriteStep.h"

#include "Expr.h"
#include "InjectedIDs.h"
#include "PathPattern.h"
#include "PipelineException.h"
#include "Profiler.h"
#include "SystemManager.h"
#include "TypeConstraint.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "versioning/CommitBuilder.h"

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
    for (const auto& e : patternProperties->getExpressions()) {
        const auto& left = static_cast<const VarExpr*>(e->getLeftExpr());
        const auto& right = static_cast<const ExprConst*>(e->getRightExpr());

        if (left->getKind() != Expr::EK_VAR_EXPR) {
            throw PipelineException("Node property expression must be an assignment");
        }

        const auto& propertyName =
            static_cast<const VarExpr*>(e->getLeftExpr())->getName();

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
    _writeBuffer->_varNodeMap[nodeVarName] = thisNodeOffset;

    return thisNodeOffset;
}

CommitWriteBuffer::ContingentNode WriteStep::getOrWriteNode(EntityPattern* nodePattern) {
    const std::string& nodeVarName = nodePattern->getVar()->getName();

    // Check to see if this node has been written already by searching its variable name
    auto pendingNodeIt = _writeBuffer->_varNodeMap.find(nodeVarName);

    if (pendingNodeIt != _writeBuffer->_varNodeMap.end()) {
        return pendingNodeIt->second; // PendingNodeOffset to the PendingNode of this node
    }

    // Check to see if @ref nodePattern has injected ID
    auto injectedIDs = nodePattern->getInjectedIDs();
    if (injectedIDs) {
        // @ref QueryPlanner::analyzeEntityPattern ensures there is exactly 1
        // injected ID (if any), set this node to have that injected ID
        return NodeID {injectedIDs->getIDs().front()};
    }

    // If this node has not been written, and it does not have a specific NodeID injected,
    // we must write it now
    CommitWriteBuffer::PendingNodeOffset newOffset = writeNode(nodePattern);
    return newOffset;
}


void WriteStep::writeEdges(const PathPattern* pathPattern) {
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
