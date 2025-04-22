#include "CreateNodeStep.h"
#include "ChangeManager.h"
#include "Expr.h"
#include "ExprConstraint.h"
#include "PathPattern.h"
#include "SystemManager.h"
#include "TypeConstraint.h"
#include "PipelineException.h"
#include "metadata/LabelSet.h"
#include "versioning/Transaction.h"
#include "versioning/CommitBuilder.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

using namespace db;

CreateNodeStep::CreateNodeStep(const EntityPattern* data)
    : _data(data) {
}

CreateNodeStep::~CreateNodeStep() {
}

void CreateNodeStep::prepare(ExecutionContext* ctxt) {
    auto changeRes = ctxt->getSystemManager()->getChangeManager().getChange(ctxt->getCommitHash());
    if (!changeRes) {
        throw PipelineException("No active change matches the requested hash");
    }

    auto& change = *changeRes.value();
    if (change.pendingCount() == 0) {
        change.newBuilder();
    }

    _builder = &change.getCurrentBuilder();
}

void CreateNodeStep::execute() {
    fmt::print("Creating node\n");
    auto& metadata = _builder->getMetadata();

    const TypeConstraint* type = _data->getTypeConstraint();
    const ExprConstraint* expr = _data->getExprConstraint();

    LabelSet labelSet;
    for (const auto& name : type->getTypeNames()) {
        labelSet.set(metadata.getOrCreateLabel(name->getName()));
    }

    if (labelSet.empty()) {
        throw PipelineException("Nodes mush have at least one label");
    }

    auto nodeID = _builder->addNode(labelSet);
    fmt::print("  Created node: {}\n", nodeID.getValue());

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
                _builder->addNodeProperty<types::Int64>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::UInt64: {
                const auto* casted = static_cast<const UInt64ExprConst*>(right);
                _builder->addNodeProperty<types::UInt64>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::Double: {
                const auto* casted = static_cast<const DoubleExprConst*>(right);
                _builder->addNodeProperty<types::Double>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::String: {
                const auto* casted = static_cast<const StringExprConst*>(right);
                _builder->addNodeProperty<types::String>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::Bool: {
                const auto* casted = static_cast<const BoolExprConst*>(right);
                _builder->addNodeProperty<types::Bool>(nodeID, propType._id, casted->getVal());
                break;
            }
            case ValueType::Invalid:
            case ValueType::_SIZE:
                throw PipelineException("Invalid value type");
        }
    }
}

void CreateNodeStep::describe(std::string& descr) const {
    descr.clear();
    descr += "CreateNodeStep";
}
