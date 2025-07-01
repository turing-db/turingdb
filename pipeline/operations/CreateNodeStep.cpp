#include "CreateNodeStep.h"
#include "Expr.h"
#include "ExprConstraint.h"
#include "PathPattern.h"
#include "Profiler.h"
#include "TypeConstraint.h"
#include "PipelineException.h"
#include "VarDecl.h"
#include "metadata/LabelSet.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

using namespace db;

CreateNodeStep::CreateNodeStep(const EntityPattern* data)
    : _data(data)
{
}

CreateNodeStep::~CreateNodeStep() {
}

void CreateNodeStep::prepare(ExecutionContext* ctxt) {
    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx->writingPendingCommit()) {
        throw PipelineException("CreateNodeStep: Cannot create node outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();

    _builder = &tx.commitBuilder()->getCurrentBuilder();
}

void CreateNodeStep::execute() {
    Profile profile {"CreateNodeStep::execute"};

    createNode(_builder, _data);
}

void CreateNodeStep::describe(std::string& descr) const {
    descr.clear();
    descr += "CreateNodeStep";
}

void CreateNodeStep::createNode(DataPartBuilder* builder, const EntityPattern* data) {
    auto& metadata = builder->getMetadata();

    const TypeConstraint* type = data->getTypeConstraint();
    const ExprConstraint* expr = data->getExprConstraint();
    auto* col = data->getVar()->getDecl()->getColumn();

    LabelSet labelSet;
    if (type) {
        for (const auto& name : type->getTypeNames()) {
            labelSet.set(metadata.getOrCreateLabel(name->getName()));
        }
    }

    if (labelSet.empty()) {
        throw PipelineException("Nodes must have at least one label");
    }

    auto nodeID = builder->addNode(labelSet);
    static_cast<ColumnNodeID*>(col)->set(nodeID);

    if (!expr) {
        return;
    }

    for (const auto& e : expr->getExpressions()) {
        const auto& left = static_cast<const VarExpr*>(e->getLeftExpr());
        const auto& right = static_cast<const ExprConst*>(e->getRightExpr());

        if (left->getKind() != Expr::EK_VAR_EXPR) {
            throw PipelineException("Node property expression must be an assignment");
        }

        // Types are checked previously in QueryAnalyzer
        const ValueType valueType = right->getType();
        const auto propType = metadata.getOrCreatePropertyType(left->getName(), valueType);

        switch (valueType) {
            case ValueType::Int64: {
                const auto* casted = static_cast<const Int64ExprConst*>(right);
                builder->addNodeProperty<types::Int64>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::UInt64: {
                const auto* casted = static_cast<const UInt64ExprConst*>(right);
                builder->addNodeProperty<types::UInt64>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::Double: {
                const auto* casted = static_cast<const DoubleExprConst*>(right);
                builder->addNodeProperty<types::Double>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::String: {
                const auto* casted = static_cast<const StringExprConst*>(right);
                builder->addNodeProperty<types::String>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::Bool: {
                const auto* casted = static_cast<const BoolExprConst*>(right);
                builder->addNodeProperty<types::Bool>(nodeID, propType._id, casted->getVal());
                break;
            }
            default: {
                throw std::runtime_error("Unsupported value type");
            }
        }
    }
}

