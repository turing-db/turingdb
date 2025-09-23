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

namespace rg = ranges;
namespace rv = rg::views;

using namespace db;

namespace {

using CWB = CommitWriteBuffer;

void addUntypedProperties(CWB::UntypedProperties& props, const BinExpr* propExpr) {
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
            props.emplace_back(propertyName, casted->getVal());
            break;
        }
        case ValueType::UInt64: {
            const auto* casted = static_cast<const UInt64ExprConst*>(right);
            props.emplace_back(propertyName, casted->getVal());
            break;
        }
        case ValueType::Double: {
            const auto* casted = static_cast<const DoubleExprConst*>(right);
            props.emplace_back(propertyName, casted->getVal());
            break;
        }
        case ValueType::String: {
            const auto* casted = static_cast<const StringExprConst*>(right);
            props.emplace_back(propertyName, casted->getVal());
            break;
        }
        case ValueType::Bool: {
            const auto* casted = static_cast<const BoolExprConst*>(right);
            props.emplace_back(propertyName, casted->getVal());
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

    _writeBuffer = &tx.commitBuilder()->writeBuffer();
}

CWB::PendingNodeOffset WriteStep::writeNode(const EntityPattern* nodePattern) {
    // Build a @ref CommitWriteBuffer::PendingNode:

    // Labels to pass to the PendingNode
    const TypeConstraint* patternLabels = nodePattern->getTypeConstraint();
    std::vector<std::string> nodeLabels;
    if (patternLabels) {
        nodeLabels.reserve(patternLabels->getTypeNames().size());
        for (const VarExpr* name : patternLabels->getTypeNames()) {
            nodeLabels.emplace_back(name->getName());
        }
    } else { // TODO: This check should be obselete as it should be checked in parser
        throw PipelineException("Nodes must have at least one label");
    }

    // Properties to pass to the PendingNode
    CWB::UntypedProperties nodeProperties;

    const ExprConstraint* patternProperties = nodePattern->getExprConstraint();
    if (!patternProperties) { // Early exit if no properties
        // Add this node to the write buffer, and record its offset
        CWB::PendingNodeOffset thisNodeOffset =
            _writeBuffer->nextPendingNodeOffset();
        const std::string& nodeVarName = nodePattern->getVar()->getName();

        _writeBuffer->addPendingNode(nodeLabels, nodeProperties);
        _varOffsetMap[nodeVarName] = thisNodeOffset;

        return thisNodeOffset;
    }

    for (const BinExpr* e : patternProperties->getExpressions()) {
        addUntypedProperties(nodeProperties, e);
    }

    // Add this node to the write buffer, and record its offset
    CWB::PendingNodeOffset thisNodeOffset =
        _writeBuffer->nextPendingNodeOffset();
    const std::string& nodeVarName = nodePattern->getVar()->getName();

    _writeBuffer->addPendingNode(nodeLabels, nodeProperties);
    _varOffsetMap[nodeVarName] = thisNodeOffset;

    return thisNodeOffset;
}

CWB::ContingentNode WriteStep::getOrWriteNode(const EntityPattern* nodePattern) {
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
    CWB::PendingNodeOffset newOffset = writeNode(nodePattern);
    return newOffset;
}

void WriteStep::writeEdge(const ContingentNode src, const ContingentNode tgt,
                          const EntityPattern* edgePattern) {
    // Get the EdgeType for PendingEdge
    const TypeConstraint* patternType = edgePattern->getTypeConstraint();
    std::string edgeType;
    if (patternType) {
            edgeType = patternType->getTypeNames().front()->getName();
    } else { // TODO: This check should be obselete as it should be checked in parser
        throw PipelineException("Edges must have at least one label");
    }

    const ExprConstraint* patternProperties = edgePattern->getExprConstraint();
    if (!patternProperties) { // No properties
        CWB::UntypedProperties emptyProps;
        _writeBuffer->addPendingEdge(src, tgt, edgeType, emptyProps);
        return;
    }

    CWB::UntypedProperties edgeProperties;
    for (const auto& e : patternProperties->getExpressions()) {
        addUntypedProperties(edgeProperties, e);
    }
    _writeBuffer->addPendingEdge(src, tgt, edgeType, edgeProperties);
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
