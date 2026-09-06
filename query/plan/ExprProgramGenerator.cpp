#include "ExprProgramGenerator.h"

#include <spdlog/fmt/fmt.h>
#include <utility>

#include "list/ListView.h"
#include "PipelineGenerator.h"
#include "columns/AllowedKinds.h"
#include "columns/BinaryOperators.h"
#include "columns/BinaryPredicates.h"
#include "columns/Column.h"
#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/Functions.h"
#include "columns/UnaryPredicates.h"
#include "dataframe/ColumnTag.h"
#include "dataframe/DataframeManager.h"
#include "decl/EvaluatedType.h"
#include "expr/Operators.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "interfaces/PipelineOutputInterface.h"
#include "processors/ExprProgram.h"
#include "processors/PredicateProgram.h"
#include "Predicate.h"

#include "expr/Expr.h"
#include "expr/BinaryExpr.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/IndexExpr.h"
#include "expr/LiteralExpr.h"
#include "FunctionInvocation.h"
#include "expr/ExprChain.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "metadata/PropertyType.h"
#include "columns/ColumnStringTable.h"

#include "dataframe/NamedColumn.h"
#include "columns/ColumnOperator.h"

#include "list/ListBuffer.h"

#include "LocalMemory.h"

#include "PlannerException.h"
#include "FatalException.h"

using namespace db;

namespace {

struct AddLiteralToList {
    std::vector<ListBuffer<>::ListItemVariant>& _items;

    template <typename T>
    void operator()(const ColumnConst<T>* litCol) {
        _items.emplace_back(litCol->getRaw());
    }
};

}

ColumnOperator ExprProgramGenerator::unaryOperatorToColumnOperator(UnaryOperator op) {
    switch (op) {
        case UnaryOperator::Not:
            return ColumnOperator::OP_NOT;
        break;

        case UnaryOperator::Minus:
            return ColumnOperator::OP_MINUS;
        break;

        case UnaryOperator::Plus:
            return ColumnOperator::OP_PLUS;
        break;

        case UnaryOperator::_SIZE:
            throw PlannerException(
                "Attempted to generate invalid unary operator in ExprProgramGenerator.");
        break;
    }
    throw FatalException(
        "Attempted to generate invalid unary operator in ExprProgramGenerator.");
}

ColumnOperator ExprProgramGenerator::binaryOperatorToColumnOperator(BinaryOperator op) {
    switch (op) {
        case BinaryOperator::Or:
            return ColumnOperator::OP_OR;
        break;

        case BinaryOperator::And:
            return ColumnOperator::OP_AND;
        break;

        case BinaryOperator::Equal:
            return ColumnOperator::OP_EQUAL;
        break;

        case BinaryOperator::NotEqual:
            return ColumnOperator::OP_NOT_EQUAL;
        break;

        case BinaryOperator::GreaterThan:
            return ColumnOperator::OP_GREATER_THAN;
        break;

        case BinaryOperator::LessThan:
            return ColumnOperator::OP_LESS_THAN;
        break;

        case BinaryOperator::GreaterThanOrEqual:
            return ColumnOperator::OP_GREATER_THAN_OR_EQUAL;
        break;

        case BinaryOperator::LessThanOrEqual:
            return ColumnOperator::OP_LESS_THAN_OR_EQUAL;
        break;

        case BinaryOperator::Add:
            return ColumnOperator::OP_ADD;
        break;

        case BinaryOperator::Sub:
            return ColumnOperator::OP_SUB;
        break;

        case BinaryOperator::Mult:
            return ColumnOperator::OP_MUL;
        break;

        case BinaryOperator::Div:
            return ColumnOperator::OP_DIV;
        break;

        case BinaryOperator::_SIZE:
            throw FatalException(
                "Attempted to generate invalid binary operator in ExprProgramGenerator.");
        break;

        default:
            throw PlannerException(fmt::format("Binary operator {} not yet supported.",
                                               BinaryOperatorDescription::value(op)));
        break;
    }
}

Column* ExprProgramGenerator::registerPropertyConstraint(const Expr* expr) {
    Column* resCol = generateExpr(expr);
    return resCol;
}

Column* ExprProgramGenerator::generateExpr(const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::UNARY:
            return generateUnaryExpr(static_cast<const UnaryExpr*>(expr));
        break;

        // TODO
        case Expr::Kind::STRING:
            throw PlannerException("String expressions are currently not supported.");
        break;

        // TODO
        case Expr::Kind::PATH:
            throw PlannerException("Path expressions are currently not supported.");
        break;

        case Expr::Kind::FUNCTION_INVOCATION:
            return generateFuncInvocationExpr(
                static_cast<const FunctionInvocationExpr*>(expr));
        break;

        // TODO
        case Expr::Kind::ENTITY_TYPES:
            throw PlannerException("Entity expressions are currently not supported.");
        break;

        case Expr::Kind::SYMBOL:
            return generateSymbolExpr(static_cast<const SymbolExpr*>(expr));
        break;

        case Expr::Kind::BINARY:
            return generateBinaryExpr(static_cast<const BinaryExpr*>(expr));
        break;

        case Expr::Kind::PROPERTY:
            return generatePropertyExpr(static_cast<const PropertyExpr*>(expr));
        break;

        case Expr::Kind::INDEX:
            return generateIndexExpr(static_cast<const IndexExpr*>(expr));
        break;

        case Expr::Kind::LITERAL:
            return generateLiteralExpr(static_cast<const LiteralExpr*>(expr));
        break;

        case Expr::Kind::LIST:
            throw PlannerException("List expressions are currently not supported.");
        break;

        case Expr::Kind::_SIZE:
            throw PlannerException("Unknown expression type in ExprProgramGenerator.");
        break;
    }

    throw FatalException("Invalid Expr type in ExprProgramGenerator.");
}

Column* ExprProgramGenerator::generateUnaryExpr(const UnaryExpr* unExpr) {
    const Expr* operand = unExpr->getSubExpr();
    const UnaryOperator optor = unExpr->getOperator();

    const ColumnOperator colOp = unaryOperatorToColumnOperator(optor);
    Column* operandColumn = generateExpr(operand);
    Column* resCol = allocUnaryResultCol(colOp, operandColumn);

    _exprProg->addInstr(colOp, resCol, operandColumn, nullptr);

    return resCol;
}

Column* ExprProgramGenerator::generateBinaryExpr(const BinaryExpr* binExpr) {
    Column* lhs = generateExpr(binExpr->getLHS());
    Column* rhs = generateExpr(binExpr->getRHS());
    const ColumnOperator op = binaryOperatorToColumnOperator(binExpr->getOperator());
    Column* resCol = allocBinaryResultCol(op, lhs, rhs);

    _exprProg->addInstr(op, resCol, lhs, rhs);

    return resCol;
}

Column* ExprProgramGenerator::generatePropertyExpr(const PropertyExpr* propExpr) {
    if (propExpr->isStringTableHeaderAccess()) {
        const VarDecl* entityDecl = propExpr->getEntityVarDecl();
        const auto it = _gen->varColMap().find(entityDecl);
        if (it == _gen->varColMap().end()) {
            throw FatalException("Could not find column for CSV row variable");
        }

        const NamedColumn* tableCol = _pendingOut.getDataframe()->getColumn(it->second);
        auto* table = static_cast<ColumnStringTable*>(tableCol->getColumn());

        const std::string_view headerName = propExpr->getPropName();
        Column* field = table->findFieldByHeader(headerName);
        if (!field) {
            throw PlannerException(fmt::format("CSV header '{}' not found", headerName));
        }
        return field;
    }

    const VarDecl* exprVarDecl = propExpr->getExprVarDecl();
    bioassert(exprVarDecl, "Null property variable");

    // Search exprVarDecl in column map
    const auto foundIt = _gen->varColMap().find(exprVarDecl);
    if (foundIt == _gen->varColMap().end()) {
        throw FatalException(
            fmt::format("Could not find column associated with property variable {}.",
                        exprVarDecl->getName()));
    }

    const NamedColumn* inCol = _pendingOut.getDataframe()->getColumn(foundIt->second);
    if (!inCol) {
        throw FatalException(fmt::format(
            "Could not get column in input to ExprProgramGenerator for variable {}.",
            foundIt->second.getValue()));
    }

    return inCol->getColumn();
}

#define GEN_LITERAL_CASE(MyKind, Type, LiteralType)                                      \
    case Literal::Kind::MyKind: {                                                        \
        ColumnConst<types::Type::Primitive>* value =                                     \
            _gen->memory().alloc<ColumnConst<types::Type::Primitive>>();                 \
        value->set(static_cast<const LiteralType*>(literal)->getValue());                \
        outCol = value;                                                                  \
    }                                                                                    \
    break;

Column* ExprProgramGenerator::generateLiteralExpr(const LiteralExpr* literalExpr) {
    Literal* literal = literalExpr->getLiteral();

    Column* outCol = nullptr;

    switch (literal->getKind()) {
        GEN_LITERAL_CASE(BOOL, Bool, BoolLiteral)
        GEN_LITERAL_CASE(INTEGER, Int64, IntegerLiteral)
        GEN_LITERAL_CASE(STRING, String, StringLiteral)
        GEN_LITERAL_CASE(DOUBLE, Double, DoubleLiteral)
        GEN_LITERAL_CASE(EMBEDDING, Embedding, EmbeddingLiteral)

        case Literal::Kind::LIST: {
            using Types = ListableTypes;
            using AddItem = ColumnSingleDispatcher<Types::Allowed,
                                                   AddLiteralToList,
                                                   Types::LiteralExcluded>;

            const Literal* lit = literalExpr->getLiteral();
            const ListLiteral* list = static_cast<const ListLiteral*>(lit);

            std::vector<ListBuffer<>::ListItemVariant> items;
            items.reserve(list->size());

            AddLiteralToList listBuilder {._items = items};

            for (const Expr* item : list->items()) {
                const Column* itemConst = generateExpr(item);
                AddItem::dispatch(itemConst, listBuilder);
            }

            QueryListBuffer& buf = _gen->memory().listBuffer();

            const ListView view = buf.insert(items);

            // TODO: Probably change to types::List::Primitive once we have list props
            auto* value = _gen->memory().alloc<ColumnConst<ListView>>();
            value->set(view);

            outCol = value;
        }
        break;

        case Literal::Kind::NULL_LITERAL: {
            auto* value = _gen->memory().alloc<ColumnConst<PropertyNull>>();
            return value;
        }
        break;

        default:
            throw PlannerException(
                fmt::format("ExprProgramGenerator: unsupported literal of type {}",
                            std::to_underlying(literal->getKind())));
        break;
    }

    bioassert(outCol, "Failed to allocate literal column");
    return outCol;
}

Column* ExprProgramGenerator::generateSymbolExpr(const SymbolExpr* symbolExpr) {
    const VarDecl* exprVarDecl = symbolExpr->getExprVarDecl();
    const EvaluatedType type = symbolExpr->getType();
    symbolExpr->getSymbol();

    // Search exprVarDecl in column map. It may not be present, in the case that this
    // variable is only manifested by a VarNode *after* this filter (see
    // `MATCH (n), (m) WHERE n <> m RETURN n, m` as an example). In this case, the
    // variable must be from the incoming stream.
    const auto foundIt = _gen->varColMap().find(exprVarDecl);

    // If we find the var in the map, use that column
    if (foundIt != _gen->varColMap().end()) {
        const NamedColumn* symCol = _pendingOut.getDataframe()->getColumn(foundIt->second);
        bioassert(symCol, "Failed to retrieve column for SymbolExpr with tag {}.",
                  foundIt->second.getValue());

        return symCol->getColumn();    
    }

    const bool isNode = type == EvaluatedType::NodePattern;
    const bool isEdge = type == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Unknown symbol type.");

    // Otherwise, var is not in the map, look in the current stream
    const auto& incomingStream = _pendingOut.getInterface()->getStream();
    const bool incStreamContainsVar = (isNode && incomingStream.isNodeStream())
                                   || (!isNode && incomingStream.isEdgeStream());
    if (!incStreamContainsVar) {
        throw FatalException(
            fmt::format("Could not find column associated with symbol variable {}.",
                        exprVarDecl->getName()));
    }

    const ColumnTag streamedVarTag = isNode ? incomingStream.asNodeStream()._nodeIDsTag
                                            : incomingStream.asEdgeStream()._edgeIDsTag;

    const NamedColumn* streamedCol =
        _pendingOut.getDataframe()->getColumn(streamedVarTag);
    return streamedCol->getColumn();
}

Column* ExprProgramGenerator::generateIndexExpr(const IndexExpr* indexExpr) {
    if (!indexExpr->hasLiteralIndex()) {
        throw PlannerException("Dynamic CSV row indexing not yet supported");
    }

    const Expr* base = indexExpr->getBase();
    const VarDecl* baseDecl = base->getExprVarDecl();
    if (!baseDecl) {
        throw PlannerException("CSV index base does not have a variable");
    }

    const auto it = _gen->varColMap().find(baseDecl);
    if (it == _gen->varColMap().end()) {
        throw PlannerException("Could not find column for CSV row variable");
    }

    const NamedColumn* tableCol = _pendingOut.getDataframe()->getColumn(it->second);
    auto* table = static_cast<ColumnStringTable*>(tableCol->getColumn());

    const size_t fieldIdx = indexExpr->getLiteralIndex();
    if (fieldIdx >= table->getFieldCount()) {
        throw PlannerException(fmt::format("CSV column index {} out of range (max {})",
                                           fieldIdx, table->getFieldCount() - 1));
    }

    return table->getFieldColumn(fieldIdx);
}

Column* ExprProgramGenerator::generateFuncInvocationExpr(const FunctionInvocationExpr* funcExpr) {
    // Aggregates are not evaluated by this program, they require their own processor,
    // which should've been scheduled in prior to this ExprProgram. Fetch the column from
    // the input dataframe.
    if (funcExpr->isAggregate()) {
        return fetchAggregateColumn(funcExpr);
    }

    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
    const FunctionSignature* signature = invocation->getSignature();

    if (!signature) {
        throw PlannerException("Function invocation does not have a signature");
    }

    const std::string_view funcName = signature->getFullName();
    const ExprChain* args = invocation->getArguments();

    if (funcName == "toInteger" || funcName == "toFloat" || funcName == "toBoolean") {
        if (args->size() != 1) {
            throw PlannerException(
                fmt::format("{}() expects 1 argument, got {}", funcName, args->size()));
        }

        Column* argCol = generateExpr(args->front());

        ColumnOperator convOp = OP_NOOP;
        Column* resCol = nullptr;

        if (funcName == "toInteger") {
            convOp = OP_TO_INTEGER;
            resCol = allocUnaryResultCol(convOp, argCol);
        } else if (funcName == "toFloat") {
            convOp = OP_TO_FLOAT;
            resCol = allocUnaryResultCol(convOp, argCol);
        } else {
            convOp = OP_TO_BOOLEAN;
            resCol = allocUnaryResultCol(convOp, argCol);
        }

        _exprProg->addInstr(convOp, resCol, argCol, nullptr);
        return resCol;
    }

    if (funcName == "labels") {
        if (args->size() != 1) {
            throw PlannerException(
                fmt::format("{}() expects 1 argument, got {}", funcName, args->size()));
        }

        Column* argCol = generateExpr(args->front());
        const ColumnOperator op = OP_FUNC_LABELS;
        Column* resCol = allocUnaryResultCol(op, argCol);

        _exprProg->addInstr(op, resCol, argCol, nullptr);
        return resCol;
    }

    if (funcName == "edgeType" || funcName == "type") {
        if (args->size() != 1) {
            throw PlannerException(
                fmt::format("{}() expects 1 argument, got {}", funcName, args->size()));
        }

        Column* argCol = generateExpr(args->front());
        const ColumnOperator op = OP_FUNC_EDGE_TYPES;
        Column* resCol = allocUnaryResultCol(op, argCol);

        _exprProg->addInstr(op, resCol, argCol, nullptr);
        return resCol;
    }

    const bool isCosineSimilarity = (funcName == "cosine_similarity");
    const bool isEuclideanDistance = (funcName == "euclidean_distance");
    if (isCosineSimilarity || isEuclideanDistance) {
        if (args->size() != 2) {
            throw PlannerException(fmt::format("{}() expects 2 arguments, got {}", funcName, args->size()));
        }

        const Expr* lhsExpr = args->getExprs()[0];
        const Expr* rhsExpr = args->getExprs()[1];

        Column* lhsCol = generateExpr(lhsExpr);
        Column* rhsCol = generateExpr(rhsExpr);

        const ColumnOperator op = isCosineSimilarity ? OP_FUNC_COSINE_SIMILARITY : OP_FUNC_EUCLIDEAN_DISTANCE;

        Column* resCol = allocBinaryResultCol(op, lhsCol, rhsCol);
        _exprProg->addInstr(op, resCol, lhsCol, rhsCol);

        return resCol;
    }

    throw PlannerException(
        fmt::format("Function '{}' is not supported in expressions", funcName));
}

#define ALLOC_EVALTYPE_COL(EvalType, Type)                                               \
    case EvalType:                                                                       \
        return _gen->memory().alloc<ColumnOptVector<Type::Primitive>>();                 \
    break;

template <ColumnOperator Op>
struct ResultAllocator {
    Column*& _resultCol;
    PipelineGenerator* _gen {nullptr};

    template <typename T>
    void operator()(const T* arg) {
        bioassert(arg, "Attempted to allocate a result column with null unary argument.");

        if constexpr (Op == OP_NOT) {
            using ResultType = typename UnaryColumnCombination<Not, T>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_TO_INTEGER) {
            using Functor = ConversionFunctorFor<toIntegerFunction, ConversionArgument<T>>::Type;
            using ResultType = FunctionColumnResult<Functor, T>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_TO_FLOAT) {
            using Functor = ConversionFunctorFor<toFloatFunction, ConversionArgument<T>>::Type;
            using ResultType = FunctionColumnResult<Functor, T>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_TO_BOOLEAN) {
            using ResultType = FunctionColumnResult<toBoolFunction, T>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_FUNC_LABELS) {
            using ResultType = FunctionColumnResult<LabelsFunction, T>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_FUNC_EDGE_TYPES) {
            using ResultType = FunctionColumnResult<EdgeTypesFunction, T>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        }
    }

    // Allocating for binary operators
    template <typename T, typename U>
    void operator()(const T* lhs, const U* rhs) {
        bioassert(lhs && rhs,
                  "Attempted to allocate a result column with null operands.");
        
        if constexpr (Op == OP_EQUAL) {
            using ResultType = typename ColumnCombination<Eq, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_NOT_EQUAL) {
            using ResultType = typename ColumnCombination<Ne, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_GREATER_THAN) {
            using ResultType = typename ColumnCombination<Gt, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_LESS_THAN) {
            using ResultType = typename ColumnCombination<Lt, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_GREATER_THAN_OR_EQUAL) {
            using ResultType = typename ColumnCombination<Gte, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_LESS_THAN_OR_EQUAL) {
            using ResultType = typename ColumnCombination<Lte, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_AND) {
            using ResultType = typename ColumnCombination<And, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_OR) {
            using ResultType = typename ColumnCombination<Or, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_ADD) {
            using ResultType = typename ColumnCombination<Add, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_SUB) {
            using ResultType = typename ColumnCombination<Sub, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_MUL) {
            using ResultType = typename ColumnCombination<Mul, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_DIV) {
            using ResultType = typename ColumnCombination<Div, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_FUNC_COSINE_SIMILARITY) {
            using ResultType = typename BinaryFunctionColumnResult<CosineSimilarityFunction, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else if constexpr (Op == OP_FUNC_EUCLIDEAN_DISTANCE) {
            using ResultType = typename BinaryFunctionColumnResult<EuclideanDistanceFunction, T, U>::ResultColumnType;
            _resultCol = _gen->memory().alloc<ResultType>();
        } else {
            throw FatalException("Unsupported allocator.");
        }
    }
};

// Uses Column dispatching to dispatch a functor which allocates the result column
#define DISPATCHER_CASE(Operator)                                                        \
    case (Operator): {                                                                   \
        ResultAllocator<Operator> allocator(result, _gen);                               \
        using Pairs = PairRestrictions<Operator>;                                        \
        ColumnDoubleDispatcher<Pairs::Allowed, Pairs::AllowedMixed,                      \
                               ResultAllocator<Operator>,                                \
                               Pairs::Excluded>::dispatch(lhs, rhs, allocator);          \
    } break;

Column* ExprProgramGenerator::allocBinaryResultCol(ColumnOperator op,
                                                   const Column* lhs,
                                                   const Column* rhs) {
    Column* result = nullptr;


    switch (op) {
        DISPATCHER_CASE(OP_EQUAL)
        DISPATCHER_CASE(OP_NOT_EQUAL);

        DISPATCHER_CASE(OP_GREATER_THAN)
        DISPATCHER_CASE(OP_LESS_THAN)
        DISPATCHER_CASE(OP_GREATER_THAN_OR_EQUAL)
        DISPATCHER_CASE(OP_LESS_THAN_OR_EQUAL)

        DISPATCHER_CASE(OP_AND)
        DISPATCHER_CASE(OP_OR)

        DISPATCHER_CASE(OP_ADD)
        DISPATCHER_CASE(OP_SUB)
        DISPATCHER_CASE(OP_MUL)
        DISPATCHER_CASE(OP_DIV)

        DISPATCHER_CASE(OP_FUNC_COSINE_SIMILARITY)
        DISPATCHER_CASE(OP_FUNC_EUCLIDEAN_DISTANCE)

        case OP_IN: // TODO: Implement
            throw PlannerException("Unsupported allocator: IN.");
        break;

        case OP_MINUS:
        case OP_PLUS:
        case OP_NOT:
            throw PlannerException(
                fmt::format("Attempted to allocate binary result for unary operator {}.",
                            ColumnOperatorDescription::value(op)));
        break;

        case OP_NOOP:
        case OP_PROJECT:
        case _SIZE:
        default:
            throw FatalException("Attempted invalid operator result allocation.");
        break;
    }

    bioassert(result, "Failed to allocate result column.");
    return result;
}

// Uses Column dispatching to dispatch a functor which allocates the result column
#define UNARY_DISPATCHER_CASE(Operator)                                                  \
    case (Operator): {                                                                   \
        using Types = TypeRestrictions<Operator>;                                        \
        ResultAllocator<Operator> allocator(result, _gen);                               \
        ColumnSingleDispatcher<typename Types::Allowed, ResultAllocator<Operator>,       \
                               typename Types::Excluded>::dispatch(arg, allocator);      \
    }                                                                                    \
    break;

Column* ExprProgramGenerator::allocUnaryResultCol(ColumnOperator op, const Column* arg) {
    Column* result = nullptr;

    switch (op) {
        UNARY_DISPATCHER_CASE(OP_NOT)
        UNARY_DISPATCHER_CASE(OP_TO_INTEGER)
        UNARY_DISPATCHER_CASE(OP_TO_FLOAT)
        UNARY_DISPATCHER_CASE(OP_TO_BOOLEAN)
        UNARY_DISPATCHER_CASE(OP_FUNC_LABELS)
        UNARY_DISPATCHER_CASE(OP_FUNC_EDGE_TYPES)

        case OP_MINUS:
        case OP_PLUS:
            throw PlannerException(
                fmt::format("Unary operator {} is not yet supported (cannot allocate).",
                            ColumnOperatorDescription::value(op)));
        break;
        
        case OP_EQUAL:
        case OP_NOT_EQUAL:
        case OP_GREATER_THAN:
        case OP_LESS_THAN:
        case OP_GREATER_THAN_OR_EQUAL:
        case OP_LESS_THAN_OR_EQUAL:
        case OP_AND:
        case OP_OR:
        case OP_ADD:
        case OP_CONCAT:
        case OP_SUB:
        case OP_MUL:
        case OP_DIV:
        case OP_MOD:
        case OP_POW:
        case OP_STARTS_WITH:
        case OP_ENDS_WITH:
        case OP_CONTAINS:
        case OP_PROJECT:
        case OP_IN:
        case OP_XOR:
            throw PlannerException(
                fmt::format("Attempted to allocate unary result for binary operator {}.",
                            ColumnOperatorDescription::value(op)));
        break;

        case OP_FUNC_COSINE_SIMILARITY:
        case OP_FUNC_EUCLIDEAN_DISTANCE:
            throw PlannerException(
                fmt::format("Attempted to allocate unary result for function {}.",
                            ColumnOperatorDescription::value(op)));
        break;

        case OP_NOOP:
        case _SIZE:
            throw FatalException("Attempted invalid operator result allocation.");
        break;
    };

    bioassert(result, "Failed to allocate result column.");
    return result;
}

Column* ExprProgramGenerator::fetchAggregateColumn(const FunctionInvocationExpr* aggregateExpr) {
    const Dataframe* inputDf = _pendingOut.getDataframe();
    bioassert(inputDf, "ExprProgram expected input, but did not have one.");

    const VarDecl* var = aggregateExpr->getExprVarDecl();
    bioassert(var, "Aggregate function invocation had no variable declaration.");

    const PipelineGenerator::VarColumnMap& varColMap = _gen->varColMap();

    const auto findIt = varColMap.find(var);
    bioassert(findIt != end(varColMap), "Aggregate function had no associated column.");

    const ColumnTag aggregateResTag = findIt->second;

    const bool foundColumn = inputDf->hasColumn(aggregateResTag);
    bioassert(foundColumn, "Could not find column for aggregate function.");

    const NamedColumn* nCol = inputDf->getColumn(aggregateResTag);
    bioassert(nCol, "Null NamedColumn of aggregate function.");

    Column* resultColumn = nCol->getColumn();
    bioassert(resultColumn, "Null result column of aggregate function.");

    return resultColumn;
}
