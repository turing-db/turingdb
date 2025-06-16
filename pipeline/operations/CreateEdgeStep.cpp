#include "CreateEdgeStep.h"
#include "Expr.h"
#include "ExprConstraint.h"
#include "PathPattern.h"
#include "Profiler.h"
#include "TypeConstraint.h"
#include "CreateNodeStep.h"
#include "PipelineException.h"
#include "VarDecl.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

using namespace db;

CreateEdgeStep::CreateEdgeStep(const EntityPattern* src,
                               const EntityPattern* edge,
                               const EntityPattern* tgt)
    : _src(src),
    _edge(edge),
    _tgt(tgt)
{
}

CreateEdgeStep::~CreateEdgeStep() {
}

void CreateEdgeStep::prepare(ExecutionContext* ctxt) {
    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx->writingPendingCommit()) {
        throw PipelineException("CreateEdgeStep: Cannot create node outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();

    _builder = &tx.commitBuilder()->getCurrentBuilder();
}

void CreateEdgeStep::execute() {
    Profile profile {"CreateEdgeStep::execute"};

    auto& metadata = _builder->getMetadata();

    const auto* srcID = static_cast<ColumnNodeID*>(_src->getVar()->getDecl()->getColumn());
    auto* edgeID = static_cast<ColumnEdgeID*>(_edge->getVar()->getDecl()->getColumn());
    auto* tgtID = static_cast<ColumnNodeID*>(_tgt->getVar()->getDecl()->getColumn());

    if (!tgtID->getRaw().isValid()) {
        CreateNodeStep::createNode(_builder, _tgt);
    }


    const TypeConstraint* type = _edge->getTypeConstraint();
    const ExprConstraint* expr = _edge->getExprConstraint();

    if (!type) {
        throw PipelineException("Edges must have a type constraint");
    }

    const std::string& edgeTypeName = type->getTypeNames().front()->getName();

    const EdgeTypeID edgeTypeID = metadata.getOrCreateEdgeType(edgeTypeName);
    const auto& edge = _builder->addEdge(edgeTypeID, srcID->getRaw(), tgtID->getRaw());
    edgeID->set(edge._edgeID);

    if (!expr) {
        return;
    }

    for (const auto& e : expr->getExpressions()) {
        const auto& left = static_cast<const VarExpr*>(e->getLeftExpr());
        const auto& right = static_cast<const ExprConst*>(e->getRightExpr());

        if (left->getKind() != Expr::EK_VAR_EXPR) {
            throw PipelineException("Node property expression must be an assignment");
        }

        const ValueType valueType = right->getType();
        const auto propType = metadata.getOrCreatePropertyType(left->getName(), valueType);

        switch (valueType) {
            case ValueType::Int64: {
                const auto* casted = static_cast<const Int64ExprConst*>(right);
                _builder->addEdgeProperty<types::Int64>(edge, propType._id, casted->getVal());
                break;
            }
            case ValueType::UInt64: {
                const auto* casted = static_cast<const UInt64ExprConst*>(right);
                _builder->addEdgeProperty<types::UInt64>(edge, propType._id, casted->getVal());
                break;
            }
            case ValueType::Double: {
                const auto* casted = static_cast<const DoubleExprConst*>(right);
                _builder->addEdgeProperty<types::Double>(edge, propType._id, casted->getVal());
                break;
            }
            case ValueType::String: {
                const auto* casted = static_cast<const StringExprConst*>(right);
                _builder->addEdgeProperty<types::String>(edge, propType._id, casted->getVal());
                break;
            }
            case ValueType::Bool: {
                const auto* casted = static_cast<const BoolExprConst*>(right);
                _builder->addEdgeProperty<types::Bool>(edge, propType._id, casted->getVal());
                break;
            }
            default: {
                throw std::runtime_error("Unsupported value type");
            }
        }
    }
}

void CreateEdgeStep::describe(std::string& descr) const {
    descr.clear();
    descr += "CreateEdgeStep";
}
