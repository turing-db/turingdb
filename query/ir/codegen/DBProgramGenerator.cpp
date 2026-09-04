#include "DBProgramGenerator.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <variant>

#include "EntityPattern.h"
#include "NodePattern.h"
#include "expr/Operators.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Interfaces/DataLayoutInterfaces.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "DBTypes.h"
#include "DBSystemProgramGenerator.h"
#include "StorageDialect.h"
#include "StorageEnums.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "CypherAST.h"
#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "Symbol.h"
#include "WhereClause.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "stmt/CallStmt.h"
#include "stmt/StmtContainer.h"
#include "decl/EvaluatedType.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "Literal.h"
#include "expr/BinaryExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/ExprChildren.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/IndexExpr.h"
#include "expr/ListExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/StructuralExpressionComparator.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "stmt/CreateStmt.h"
#include "stmt/DeleteStmt.h"
#include "expr/ExprChain.h"
#include "stmt/Limit.h"
#include "stmt/LoadCSVStmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/ReturnStmt.h"
#include "stmt/SetItem.h"
#include "stmt/SetStmt.h"
#include "stmt/Skip.h"
#include "stmt/StmtContainer.h"
#include "stmt/UnwindStmt.h"
#include "stmt/VectorSearchStmt.h"
#include "stmt/WithStmt.h"
#include "Symbol.h"
#include "SymbolChain.h"

#include "columns/BinaryOperators.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace db;

namespace {

// The name a VECTOR SEARCH gives the metric column; anything else it yields is the
// neighbour ID column, the analyzer having rejected every other name.
constexpr std::string_view vectorSearchScoreYield = "score";

using UnaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder,
                                             mlir::Location loc,
                                             mlir::db::ColumnType resultType,
                                             mlir::Value input);

template <typename Op>
mlir::Value emitUnaryFunction(mlir::OpBuilder& builder,
                              mlir::Location loc,
                              mlir::db::ColumnType resultType,
                              mlir::Value input) {
    return builder.create<Op>(loc, resultType, input).getResult();
}

const std::unordered_map<std::string_view, UnaryFunctionEmitter> unaryFunctionEmitters = {
    {"labels", &emitUnaryFunction<mlir::db::Labels>},
    {"edgeType", &emitUnaryFunction<mlir::db::EdgeType>},
    {"toInteger", &emitUnaryFunction<mlir::db::ToInteger>},
    {"toFloat", &emitUnaryFunction<mlir::db::ToFloat>},
    {"toBoolean", &emitUnaryFunction<mlir::db::ToBoolean>},
};

using BinaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder,
                                              mlir::Location loc,
                                              mlir::db::ColumnType resultType,
                                              mlir::Value lhs,
                                              mlir::Value rhs);

template <typename Op>
mlir::Value emitBinaryFunction(mlir::OpBuilder& builder,
                               mlir::Location loc,
                               mlir::db::ColumnType resultType,
                               mlir::Value lhs,
                               mlir::Value rhs) {
    return builder.create<Op>(loc, resultType, lhs, rhs).getResult();
}

const std::unordered_map<std::string_view, BinaryFunctionEmitter> binaryFunctionEmitters = {
    {"cosine_similarity", &emitBinaryFunction<mlir::db::CosineSimilarity>},
    {"euclidean_distance", &emitBinaryFunction<mlir::db::EuclideanDistance>},
};

// The list a literal UNWIND spreads, null for the one literal that is no list and still
// analyzes - `UNWIND null`, which spreads into no row. Anything else here is a statement
// the analyzer let through and neither of the sources built from one can lower.
const ListLiteral* literalUnwindList(const UnwindStmt* unwind) {
    const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(unwind->arg());
    if (!literalExpr) {
        throw TuringException("Non-literal UNWIND expressions are not yet supported.");
    }

    const Literal* literal = literalExpr->getLiteral();
    if (literal->getKind() == Literal::Kind::NULL_LITERAL) {
        return nullptr;
    }

    const ListLiteral* list = dynamic_cast<const ListLiteral*>(literal);
    if (!list) {
        throw TuringException("Non-list arguments to UNWIND are not yet supported.");
    }

    return list;
}

// True when a value is one of `columns` or is computed from one.
bool readsAnyColumn(mlir::ValueRange values, llvm::ArrayRef<mlir::Value> columns) {
    llvm::DenseSet<mlir::Value> columnSet;
    for (const mlir::Value column : columns) {
        columnSet.insert(column);
    }

    llvm::SmallVector<mlir::Value> worklist(values.begin(), values.end());
    llvm::DenseSet<mlir::Value> visited;

    while (!worklist.empty()) {
        const mlir::Value value = worklist.pop_back_val();
        if (!visited.insert(value).second) {
            continue;
        }

        if (columnSet.contains(value)) {
            return true;
        }

        mlir::Operation* const definingOp = value.getDefiningOp();
        if (definingOp) {
            worklist.append(definingOp->operand_begin(), definingOp->operand_end());
        }
    }

    return false;
}

// The variables a YIELD binds.
void collectYieldVariables(const YieldClause* yield, llvm::SmallVectorImpl<const VarDecl*>& variables) {
    const YieldItems* yieldItems = yield ? yield->getItems() : nullptr;
    if (!yieldItems) {
        return;
    }

    for (const SymbolExpr* item : *yieldItems) {
        variables.push_back(item->getDecl());
    }
}

// The YIELD of a statement that binds variables of its own: a CALL or a VECTOR SEARCH.
const YieldClause* yieldClauseOf(const Stmt* stmt) {
    const Stmt::Kind kind = stmt->getKind();

    if (kind == Stmt::Kind::CALL) {
        return static_cast<const CallStmt*>(stmt)->getYield();
    }

    bioassert(kind == Stmt::Kind::VECTOR_SEARCH, "Only a CALL or a VECTOR SEARCH binds a YIELD.");

    return static_cast<const VectorSearchStmt*>(stmt)->getYield();
}

// The variables a statement binds of its own: the return values a YIELD names, and the
// element an UNWIND spreads.
void collectStatementVariables(const Stmt* stmt, llvm::SmallVectorImpl<const VarDecl*>& variables) {
    if (stmt->getKind() == Stmt::Kind::UNWIND) {
        variables.push_back(static_cast<const UnwindStmt*>(stmt)->getDecl());
        return;
    }

    collectYieldVariables(yieldClauseOf(stmt), variables);
}

// The ops values are transitively defined by: the chain that has to travel with them for
// them to stay usable where they land.
void collectDefiningOps(mlir::ValueRange values, llvm::DenseSet<mlir::Operation*>& ops) {
    llvm::SmallVector<mlir::Value> worklist(values.begin(), values.end());

    while (!worklist.empty()) {
        const mlir::Value value = worklist.pop_back_val();

        mlir::Operation* const definingOp = value.getDefiningOp();
        if (!definingOp || !ops.insert(definingOp).second) {
            continue;
        }

        worklist.append(definingOp->operand_begin(), definingOp->operand_end());
    }
}

bool producesEdgeVar(const DependencyEdge* e) {
    const EdgeMetadata::EdgeType producedType = e->data().type();
    const bool getOut = producedType == EdgeMetadata::EdgeType::GET_OUT_EDGES;
    const bool getIn = producedType == EdgeMetadata::EdgeType::GET_IN_EDGES;
    const bool getEdges = producedType == EdgeMetadata::EdgeType::GET_EDGES;
    return getOut || getIn || getEdges;
}

bool producesNodeVar(const DependencyEdge* e) {
    const EdgeMetadata::EdgeType producedType = e->data().type();
    const bool getTgt = producedType == EdgeMetadata::EdgeType::GET_EDGE_TGT;
    const bool getSrc = producedType == EdgeMetadata::EdgeType::GET_EDGE_SRC;
    return getTgt || getSrc;
}

EdgeMetadata::EdgeType reverseEdge(EdgeMetadata::EdgeType type) {
    switch (type) {
        case EdgeMetadata::EdgeType::GET_OUT_EDGES:
            return EdgeMetadata::EdgeType::GET_IN_EDGES;
        break;

        case EdgeMetadata::EdgeType::GET_IN_EDGES:
            return db::EdgeMetadata::EdgeType::GET_OUT_EDGES;
        break;

        case EdgeMetadata::EdgeType::GET_EDGES:
            return db::EdgeMetadata::EdgeType::GET_EDGES;
        break;

        default:
            throw FatalException("Invalid attempt to reverse direction");
        break;
    }

    throw FatalException("Uncaught edge type.");

}

// A constant column holds the same value in every row, so a step shaped by rows - a
// dedup, a sort, a cut - is not given one: reading it would anchor that step where the
// constant is bound, above the loop the other columns are read in
void collectRowColumns(const Projection* projection,
                       const llvm::SmallVectorImpl<mlir::Value>& projected,
                       llvm::SmallVectorImpl<size_t>& items,
                       llvm::SmallVectorImpl<mlir::Value>& columns) {
    bioassert(projected.size() == projection->items().size(),
              "One projected column per return item expected");

    for (size_t itemIndex = 0; itemIndex < projected.size(); itemIndex++) {
        const mlir::Value column = projected[itemIndex];
        if (yieldsConstantColumn(column)) {
            continue;
        }

        items.push_back(itemIndex);
        columns.push_back(column);
    }
}

// The columns a SKIP or a LIMIT is charged to: the ones that carry rows, or the whole
// projection when it is constants alone - one row repeated, which the cut still cuts
void collectCutColumns(const Projection* projection,
                       const llvm::SmallVectorImpl<mlir::Value>& projected,
                       llvm::SmallVectorImpl<size_t>& items,
                       llvm::SmallVectorImpl<mlir::Value>& columns) {
    collectRowColumns(projection, projected, items, columns);
    if (!columns.empty()) {
        return;
    }

    for (size_t itemIndex = 0; itemIndex < projected.size(); itemIndex++) {
        items.push_back(itemIndex);
        columns.push_back(projected[itemIndex]);
    }
}

// The one type every element attribute shares, or a null type when they differ or the
// list is empty - the homogeneity verdict db.unwind_const and db.const_list read, which
// decides whether the elements ride a column of that type or a type-erased one of tagged
// scalars. A null and a nested list carry no type, so a list holding one is type-erased.
mlir::Type sharedAttrType(llvm::ArrayRef<mlir::Attribute> elements) {
    if (elements.empty()) {
        return nullptr;
    }

    const mlir::TypedAttr firstElement = mlir::dyn_cast<mlir::TypedAttr>(elements.front());
    if (!firstElement) {
        return nullptr;
    }

    const mlir::Type firstType = firstElement.getType();

    const auto hasFirstType = [firstType](mlir::Attribute element) {
        const mlir::TypedAttr typedElement = mlir::dyn_cast<mlir::TypedAttr>(element);
        return typedElement && typedElement.getType() == firstType;
    };

    return std::ranges::all_of(elements, hasFirstType) ? firstType : nullptr;
}

mlir::Value findVarOrThrow(const DBProgramGenerator::VariableIdentityMap& map,
                        const VariableDependency* var) {
    const auto findIt = map.find(var);
    bioassert(findIt != end(map), "Missing value for {}.", var->getName());
    const DBProgramGenerator::VariableIdentities& identities = findIt->second;
    bioassert(!identities.empty(), "Missing identity for {}.", var->getName());
    return identities.back();
}

void flattenConjuncts(const Expr* expr, std::vector<const Expr*>& conjuncts) {
    if (expr->getKind() == Expr::Kind::BINARY) {
        const BinaryExpr* binaryExpr = static_cast<const BinaryExpr*>(expr);
        if (binaryExpr->getOperator() == BinaryOperator::And) {
            flattenConjuncts(binaryExpr->getLHS(), conjuncts);
            flattenConjuncts(binaryExpr->getRHS(), conjuncts);
            return;
        }
    }

    conjuncts.push_back(expr);
}

int64_t applyConstantUnary(UnaryOperator op, int64_t operand) {
    switch (op) {
        case UnaryOperator::Plus:
            return operand;
        break;

        case UnaryOperator::Minus:
            return -operand;
        break;

        default:
            throw TuringException(fmt::format("Unsupported unary operator in SKIP/LIMIT expression: {}",
                                              UnaryOperatorDescription::value(op)));
        break;
    }
}

int64_t applyConstantBinary(BinaryOperator op, int64_t lhs, int64_t rhs) {
    switch (op) {
        case BinaryOperator::Add:
            return Add {}(lhs, rhs);
        break;

        case BinaryOperator::Sub:
            return Sub {}(lhs, rhs);
        break;

        case BinaryOperator::Mult:
            return Mul {}(lhs, rhs);
        break;

        case BinaryOperator::Div:
            return Div {}(lhs, rhs);
        break;

        case BinaryOperator::Mod:
            return Mod {}(lhs, rhs);
        break;

        default:
            throw TuringException(fmt::format("Unsupported operator in SKIP/LIMIT expression: {}",
                                              BinaryOperatorDescription::value(op)));
        break;
    }
}

int64_t evaluateConstantInteger(const Expr* expr) {
    const Expr::Kind kind = expr->getKind();

    switch (kind) {
        case Expr::Kind::LITERAL: {
            const LiteralExpr* literalExpr = static_cast<const LiteralExpr*>(expr);
            const Literal* literal = literalExpr->getLiteral();

            if (literal->getKind() != Literal::Kind::INTEGER) {
                throw TuringException("SKIP/LIMIT expression must evaluate to an integer");
            }

            const IntegerLiteral* integerLiteral = static_cast<const IntegerLiteral*>(literal);
            return integerLiteral->getValue();
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unaryExpr = static_cast<const UnaryExpr*>(expr);
            const int64_t operand = evaluateConstantInteger(unaryExpr->getSubExpr());
            return applyConstantUnary(unaryExpr->getOperator(), operand);
        }
        break;

        case Expr::Kind::BINARY: {
            const BinaryExpr* binaryExpr = static_cast<const BinaryExpr*>(expr);
            const int64_t lhs = evaluateConstantInteger(binaryExpr->getLHS());
            const int64_t rhs = evaluateConstantInteger(binaryExpr->getRHS());
            return applyConstantBinary(binaryExpr->getOperator(), lhs, rhs);
        }
        break;

        default:
            throw TuringException(fmt::format("Unsupported expression in SKIP/LIMIT: {}",
                                              ExprKindDescription::value(kind)));
        break;
    }
}

bool isCollectInvocation(const Expr* item) {
    if (item->getKind() != Expr::Kind::FUNCTION_INVOCATION) {
        return false;
    }

    const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(item);
    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();

    return invocation->getSignature()->getFullName() == "collect";
}

// The collects that dedupe, by their position among the collected columns: db.collect
// names them that way rather than carrying a flag, since one projection may collect a
// column both ways
void collectDistinctValueIndices(llvm::ArrayRef<const FunctionInvocationExpr*> collects,
                                 llvm::SmallVectorImpl<int64_t>& distinctValues) {
    for (size_t collectIndex = 0; collectIndex < collects.size(); collectIndex++) {
        const FunctionInvocation* invocation = collects[collectIndex]->getFunctionInvocation();

        if (invocation->isDistinct()) {
            distinctValues.push_back(static_cast<int64_t>(collectIndex));
        }
    }
}

void collectProjectedCollects(const Projection* projection,
                              llvm::SmallVectorImpl<const FunctionInvocationExpr*>& found) {
    for (const Projection::ReturnItem& returnItem : projection->items()) {
        Expr* const* itemPtr = std::get_if<Expr*>(&returnItem);
        if (!itemPtr) {
            continue;
        }

        if (isCollectInvocation(*itemPtr)) {
            found.push_back(static_cast<const FunctionInvocationExpr*>(*itemPtr));
        }
    }
}

}

DBProgramGenerator::DBProgramGenerator(mlir::ModuleOp* mainModule)
    : _module(mainModule),
    _mlirCtxt(_module->getContext()),
    _opBuilder(_module->getBodyRegion())
{
}

DBProgramGenerator::~DBProgramGenerator() {
}

mlir::db::ColumnType DBProgramGenerator::allocColumnType(mlir::Type type) {
    return mlir::db::ColumnType::get(_mlirCtxt, type);
}

void DBProgramGenerator::registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val) {
    _part._varMap[var].emplace_back(val);
}

void DBProgramGenerator::rebindYieldedColumn(const VarDecl* decl, mlir::TypedValue<mlir::Type> val) {
    for (YieldedColumn& yielded : _part._yieldedColumns) {
        if (yielded._decl == decl) {
            yielded._column = val;
        }
    }
}

void DBProgramGenerator::addScanNodes(const VariableDependency* var) {
    bioassert(!_part._varMap.contains(var), "ScanNodes for registered variable");

    const auto col = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    auto scan = _opBuilder.create<mlir::db::ScanNodes>(_opBuilder.getUnknownLoc(), col);

    registerValue(var, scan.getResult());
}

void DBProgramGenerator::addYieldedColumn(const VariableDependency* var, mlir::Value column) {
    bioassert(!_part._varMap.contains(var), "Yielded column for registered variable");

    registerValue(var, column);

    // The variable owns those rows now. Leaving them among the yields as well would give one
    // column two names, and only the variable's is rebound as the expansion replicates it.
    const auto declaresVar = [&](const YieldedColumn& yielded) {
        return yielded._decl == var->getDecl();
    };

    std::erase_if(_part._yieldedColumns, declaresVar);
}

void DBProgramGenerator::addConstScanNodes(const VariableDependency* var, const UnwindStmt* unwind) {
    bioassert(!_part._varMap.contains(var), "ConstScanNodes for registered variable");

    const ListLiteral* list = literalUnwindList(unwind);

    llvm::SmallVector<mlir::Attribute> elements;
    if (list) {
        translateListElements(list, elements);
    }

    llvm::SmallVector<int64_t> nodeIDs;
    nodeIDs.reserve(elements.size());

    for (const mlir::Attribute element : elements) {
        const mlir::IntegerAttr nodeID = mlir::dyn_cast<mlir::IntegerAttr>(element);
        if (!nodeID) {
            throw TuringException("Only node IDs can be unwound into a node pattern.");
        }

        nodeIDs.push_back(nodeID.getInt());
    }

    const mlir::db::ColumnType nodeColumnType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));

    mlir::db::ConstScanNodes constScan = _opBuilder.create<mlir::db::ConstScanNodes>(_opBuilder.getUnknownLoc(),
                                                                                    nodeColumnType,
                                                                                    nodeIDs);

    registerValue(var, constScan.getResult());
    _part._seededVars.insert(var);
}

void DBProgramGenerator::addUnwindConst(const VariableDependency* var, const UnwindStmt* unwind) {
    bioassert(!_part._varMap.contains(var), "UnwindConst for registered variable");

    const ListLiteral* list = literalUnwindList(unwind);

    llvm::SmallVector<mlir::Attribute> elements;
    if (list) {
        translateListElements(list, elements);
    }

    const mlir::Type sharedType = sharedAttrType(elements);
    const mlir::Type elementType = sharedType ? sharedType
                                              : mlir::storage::ListElementType::get(_mlirCtxt);

    const mlir::db::ColumnType resultType = allocColumnType(elementType);
    const mlir::ArrayAttr elementsAttr = _opBuilder.getArrayAttr(elements);

    mlir::db::UnwindConst unwindConst = _opBuilder.create<mlir::db::UnwindConst>(_opBuilder.getUnknownLoc(),
                                                                                resultType,
                                                                                elementsAttr);

    registerValue(var, unwindConst.getResult());
}

void DBProgramGenerator::translateListElements(const ListLiteral* list,
                                               llvm::SmallVectorImpl<mlir::Attribute>& elements) {
    const ListLiteral::Items& items = list->items();
    elements.reserve(items.size());

    for (const Expr* item : items) {
        const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(item);
        if (!literalExpr) {
            throw TuringException("Only literal elements are supported in a list.");
        }

        elements.push_back(listElementAttr(literalExpr->getLiteral()));
    }
}

mlir::Attribute DBProgramGenerator::literalAttr(const Literal* literal) {
    const Literal::Kind kind = literal->getKind();

    if (kind == Literal::Kind::NULL_LITERAL) {
        return _opBuilder.getUnitAttr();
    } else if (kind == Literal::Kind::EMBEDDING) {
        return embeddingLiteralAttr(static_cast<const EmbeddingLiteral*>(literal));
    } else if (kind == Literal::Kind::LIST) {
        const ListLiteral* nested = static_cast<const ListLiteral*>(literal);

        llvm::SmallVector<mlir::Attribute> nestedElements;
        translateListElements(nested, nestedElements);

        return _opBuilder.getArrayAttr(nestedElements);
    }

    return scalarLiteralAttr(literal);
}

mlir::Attribute DBProgramGenerator::listElementAttr(const Literal* literal) {
    const mlir::Attribute element = literalAttr(literal);
    if (!element) {
        throw TuringException("Only booleans, integers, floats, strings, nulls, embeddings and "
                              "lists are supported as list elements.");
    }

    return element;
}

mlir::Value DBProgramGenerator::translateListLiteral(const ListLiteral* list) {
    llvm::SmallVector<mlir::Attribute> elements;
    translateListElements(list, elements);

    // A list literal is a constant like any other, carried as the array of its elements.
    // The column type is db.constant's to infer: an array attribute has none of its own, so
    // the op reads the homogeneity verdict off the elements.
    mlir::db::ConstantOp constant = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                           _opBuilder.getArrayAttr(elements));

    return constant.getResult();
}

template<typename EdgeOp>
void DBProgramGenerator::addEdgeTraversal(const VariableDependency* src,
                                          const VariableDependency* edge,
                                          const VariableDependency* tgt,
                                          const std::vector<const VariableDependency*>& carrySet) {
    walkEdge<EdgeOp>(src, edge, tgt, carrySet, nullptr);
}

template<typename EdgeOp>
mlir::Value DBProgramGenerator::addJoiningEdgeTraversal(const VariableDependency* src,
                                                        const VariableDependency* edge,
                                                        const VariableDependency* tgt,
                                                        const std::vector<const VariableDependency*>& carrySet) {
    mlir::Value landed;
    walkEdge<EdgeOp>(src, edge, tgt, carrySet, &landed);

    return landed;
}

template<typename EdgeOp>
void DBProgramGenerator::walkEdge(const VariableDependency* src,
                                  const VariableDependency* edge,
                                  const VariableDependency* tgt,
                                  const std::vector<const VariableDependency*>& carrySet,
                                  mlir::Value* joinedTarget) {
    static_assert(std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetInEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetEdges>, "Invalid op");

    bioassert(src, "Null source");
    bioassert(tgt, "Null target");

    bioassert(_part._varMap.contains(src), "Edge traversal without source");

    const auto srcs = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto eids = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const auto etypes = allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));
    const auto tgts = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));

    const mlir::Value input = _part._varMap[src].back();

    llvm::SmallVector<const VariableDependency*> carried;
    llvm::SmallVector<mlir::Value> operands {input};
    llvm::SmallVector<mlir::Type> results {srcs, eids, etypes, tgts};
    for (const VariableDependency* var : carrySet) {
        // source variable is explicitly filtered by the edge op
        if (var == src) {
            continue;
        }

        const mlir::Value column = _part._varMap[var].back();
        carried.push_back(var);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    // Find the edge types to carry which were defined in this block
    llvm::SmallVector<const VariableDependency*> carriedEdgeTypes;
    for (auto& [edgeVar, column] : _part._edgeTypeMap) {
        if (!isRowAlignedHere(column)) {
            continue;
        }
        carriedEdgeTypes.push_back(edgeVar);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    // A column a CALL driving this traversal yielded is in flight here too: the expansion
    // replicates a row once per edge, so it comes along or it stops matching the rows beside
    // it. Nothing yielded is in flight when a call has yet to run, which is every traversal
    // the calls do not drive.
    llvm::SmallVector<size_t> carriedYields;
    for (size_t yieldedIndex = 0; yieldedIndex < _part._yieldedColumns.size(); yieldedIndex++) {
        const mlir::Value column = _part._yieldedColumns[yieldedIndex]._column;
        if (!isRowAlignedHere(column)) {
            continue;
        }
        carriedYields.push_back(yieldedIndex);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    // A column a barrier published rides the expansion too, when the traversal it drives is
    // generated where that column was bound: the hop replicates a row once per edge, so it
    // comes along or it stops matching the rows beside it. A published pattern variable is
    // walked by the component instead and is already in the carry set.
    llvm::SmallVector<const VariableDependency*> carriedBound;
    for (const VariableDependency* var : _vdg.boundVars()) {
        const bool walkedHere = var == src || llvm::is_contained(carrySet, var);
        if (walkedHere) {
            continue;
        }

        const mlir::Value column = _part._varMap[var].back();
        if (!isRowAlignedHere(column) || yieldsConstantColumn(column)) {
            continue;
        }

        carriedBound.push_back(var);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    const auto loc = _opBuilder.getUnknownLoc();
    auto op = _opBuilder.create<EdgeOp>(loc, results, operands);

    const mlir::Value newSrcs = op.getResult(0);
    const mlir::Value newEdges = op.getResult(1);
    const mlir::Value newEtypes = op.getResult(2);
    const mlir::Value newTgts = op.getResult(3);

    _part._edgeTypeMap[edge] = newEtypes;

    constexpr bool forwardOrientation = std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                                        or std::is_same_v<EdgeOp, mlir::db::GetEdges>;
    const mlir::Value sourceColumn = forwardOrientation ? newSrcs : newTgts;
    const mlir::Value targetColumn = forwardOrientation ? newTgts : newSrcs;

    registerValue(src, sourceColumn);
    registerValue(edge, newEdges);

    if (joinedTarget) {
        *joinedTarget = targetColumn;
    } else {
        registerValue(tgt, targetColumn);
    }

    // Register the new values of the carry set, appearing starting from index 4 in the
    // result range
    constexpr size_t GET_X_EDGES_RES_SIZE = 4;
    for (size_t i = 0; i < carried.size(); i++) {
        const size_t resultIndex = GET_X_EDGES_RES_SIZE + i;
        registerValue(carried[i], op.getResult(resultIndex));
    }

    // Update the new edge type vars for carried edges
    const size_t edgeTypeOffset = GET_X_EDGES_RES_SIZE + carried.size();
    for (size_t i = 0; i < carriedEdgeTypes.size(); i++) {
        _part._edgeTypeMap[carriedEdgeTypes[i]] = op.getResult(edgeTypeOffset + i);
    }

    const size_t yieldOffset = edgeTypeOffset + carriedEdgeTypes.size();
    for (size_t i = 0; i < carriedYields.size(); i++) {
        _part._yieldedColumns[carriedYields[i]]._column = op.getResult(yieldOffset + i);
    }

    const size_t boundOffset = yieldOffset + carriedYields.size();
    for (size_t i = 0; i < carriedBound.size(); i++) {
        registerValue(carriedBound[i], op.getResult(boundOffset + i));
    }
}

void DBProgramGenerator::generate(const CypherAST* ast) {
    bioassert(_module, "Null module");
    bioassert(_mlirCtxt, "Null context");

    _mlirCtxt->loadDialect<mlir::db::DB>();
    _mlirCtxt->loadDialect<mlir::storage::Storage>();
    _mlirCtxt->loadDialect<mlir::func::FuncDialect>();
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    { // Create main
        _opBuilder.setInsertionPointToEnd(_module->getBody());
        const mlir::FunctionType funcType = mlir::FunctionType::get(_mlirCtxt, {}, {});
        auto func = _opBuilder.create<mlir::func::FuncOp>(uloc, "main", funcType);
        mlir::Block& block = *func.addEntryBlock();
        _opBuilder.setInsertionPointToStart(&block);
    }

    if (generateSystemCommand(ast)) {
        _opBuilder.create<mlir::func::ReturnOp>(uloc);
        return;
    }

    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const SinglePartQuery* query = dynamic_cast<const SinglePartQuery*>(queries.front());
    if (!query) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    generateQueryParts(query);

    const ReturnStmt* returnStmt = query->getReturnStmt();
    const Projection* projection = returnStmt ? returnStmt->getProjection() : nullptr;

    if (projection) {
        generateGroupAggregate(projection);
    }

    generateCreate(query);
    generateSet(query);
    generateDelete(query);

    if (projection) {
        generateOutput(projection);
    } else {
        generateYieldedOutput(query);
    }

    _opBuilder.create<mlir::func::ReturnOp>(uloc);

    runPasses();
}

void DBProgramGenerator::generateQueryParts(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    if (!readStmts) {
        return;
    }

    const std::span<Stmt* const> stmts {readStmts->stmts()};

    // A WITH closes a query part: the statements before it feed its projection, and the
    // columns that projection publishes are all the statements after it can read
    size_t partBegin = 0;
    for (size_t index = 0; index < stmts.size(); index++) {
        const Stmt* stmt = stmts[index];

        if (stmt->getKind() == Stmt::Kind::WITH) {
            generatePart(stmts.subspan(partBegin, index - partBegin));
            generateWith(static_cast<const WithStmt*>(stmt));
            partBegin = index + 1;
        } else if (closesPartOnItsCut(stmt, stmts.subspan(index + 1))) {
            generatePart(stmts.subspan(partBegin, index + 1 - partBegin));
            publishInFlightColumns();
            partBegin = index + 1;
        }
    }

    if (partBegin < stmts.size()) {
        generatePart(stmts.subspan(partBegin));
    }
}

bool DBProgramGenerator::closesPartOnItsCut(const Stmt* stmt, std::span<Stmt* const> following) const {
    if (stmt->getKind() != Stmt::Kind::MATCH) {
        return false;
    }

    const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
    const bool carriesACut = matchStmt->hasOrderBy() || matchStmt->hasSkip() || matchStmt->hasLimit();

    if (!carriesACut) {
        return false;
    }

    for (const Stmt* next : following) {
        const Stmt::Kind kind = next->getKind();

        if (kind == Stmt::Kind::WITH) {
            return false;
        } else if (kind == Stmt::Kind::MATCH || kind == Stmt::Kind::UNWIND) {
            return true;
        } else if (kind == Stmt::Kind::UNWIND) {
            // A literal UNWIND opens a dataflow of its own, multiplying the rows the cut
            // applies to exactly as a following MATCH does. One evaluated per row does not.
            const UnwindStmt* unwindStmt = static_cast<const UnwindStmt*>(next);
            if (unwindStmt->unwindsLiteral()) {
                return true;
            }
        }
    }

    return false;
}

void DBProgramGenerator::generatePart(std::span<Stmt* const> stmts) {
    _vdg.build(stmts);

    generateLeadingYields(stmts);
    generateTraversal(stmts);
    throwOnUnboundPatternVariable();
    throwOnDroppedUnwindSeed();
    closeBoundMerges();
    resolveEdgeIdentities();
    generateCSVLoads(stmts);
    generateStatementOperations(stmts);
    resolveYieldedIdentities();
}

bool DBProgramGenerator::generateSystemCommand(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    DBSystemProgramGenerator systemGenerator(&_opBuilder);

    return systemGenerator.generate(queries.front());
}

void DBProgramGenerator::runPasses() {
    mlir::PassManager passManager(_mlirCtxt);
    passManager.addPass(mlir::db::createFuseScanByLabel());
    passManager.addPass(mlir::db::createPushDownFilters());
    passManager.addPass(mlir::db::createFuseUnwindEquality());
    passManager.addPass(mlir::db::createFuseScanByNodeIDs());
    passManager.addPass(mlir::db::createFuseScanEdges());
    passManager.addPass(mlir::db::createTrimUnreadColumns());

    if (mlir::failed(passManager.run(*_module))) {
        throw FatalException("DB pass pipeline failed");
    }
}

bool DBProgramGenerator::isValidRoot(const VariableDependency& var) const {
    const auto isEdgeTgtMetaVar = [](const DependencyEdge* e) -> bool {
        return e->isMetaEdge();
    };

    // A valid root is a non-meta Cypher variable which is a node
    return std::ranges::none_of(var.incoming(), [&](const DependencyEdge* e) {
        return producesEdgeVar(e) || isEdgeTgtMetaVar(e);
    });
}

void DBProgramGenerator::collectComponentRoots(llvm::SmallVectorImpl<const VariableDependency*>& roots) const {
    // Every variable a component holds can be a valid root of its own - the target of an
    // expansion has no incoming edge that produces an edge variable either - so a component
    // is opened by the first of them the graph lists, and the rest are reached from it.
    std::unordered_set<const VariableDependency*> visited;

    for (const VariableDependency& var : _vdg.vars()) {
        const bool opensComponent = !visited.contains(&var) && isValidRoot(var);
        if (!opensComponent) {
            continue;
        }

        roots.push_back(&var);

        llvm::SmallVector<const VariableDependency*> worklist {&var};
        while (!worklist.empty()) {
            const VariableDependency* const current = worklist.pop_back_val();
            if (!visited.insert(current).second) {
                continue;
            }

            for (const DependencyEdge* edge : current->edges()) {
                worklist.push_back(edge->src() == current ? edge->tgt() : edge->src());
            }
        }
    }
}

void DBProgramGenerator::generateTraversal(std::span<Stmt* const> stmts) {
    if (_vdg.empty()) {
        return;
    }

    // Main block is saved so we can splice into it after generation
    mlir::Block* const mainBlock = _opBuilder.getInsertionBlock();

    DefinedVars defined;

    // The dataflow already standing in the main block: the component a leading call drives
    // from a column it bound there, or the one a barrier left behind, extended by the hops
    // of this part.
    TranslatedComponent mainComponent;

    const VariableDependencyGraph::BoundVars& bound = _vdg.boundVars();
    if (_part._drivenRoot) {
        // A column the barrier published holds its rows already, so it opens no component
        // of its own: it stands in the main block beside the rows the leading statements
        // bound, and rides with them into any island crossed with them.
        for (const VariableDependency* var : bound) {
            defined.insert(var);

            if (!yieldsConstantColumn(_part._varMap.at(var).back())) {
                mainComponent._vars.push_back(var);
            }
        }

        translateComponent(_part._drivenRoot, defined, mainComponent._vars);
    } else if (!bound.empty()) {
        throwOnRematchedBoundEdge();
        extendBoundDataflow(defined, mainComponent._vars);
    }

    // Connected components
    std::vector<TranslatedComponent> components;

    // TODO: Use nodes at ends of diameter
    for (const VariableDependency& root : _vdg.vars()) {
        if (defined.contains(&root)) {
            continue;
        }

        if (!isValidRoot(root)) {
            continue;
        }

        TranslatedComponent& component = components.emplace_back();
        component._region = std::make_unique<mlir::Region>();

        mlir::Block* const scratch = new mlir::Block();
        component._region->push_back(scratch); // Region destructor frees scratch
        _opBuilder.setInsertionPointToStart(scratch);

        translateComponent(&root, defined, component._vars);

        for (const VariableDependency* var : component._vars) {
            bioassert(_part._varMap.contains(var), "Component var {} not registered", var->getName());
            component._columns.push_back(_part._varMap[var].back());
        }
    }

    if (components.empty()) {
        return;
    }

    // A pattern naming none of the variables already flowing matches on its own, so its
    // rows are crossed with theirs. A scope of constants alone carries no rows to cross:
    // one value stands for every row of either factor
    if (!mainComponent._vars.empty()) {
        takeMainDataflow(mainBlock, mainComponent);
        components.insert(components.begin(), std::move(mainComponent));
    }

    // Single connected component, no need to X prod any islands
    if (components.size() == 1) {
        TranslatedComponent& comp = components.front();

        const auto& reg = comp._region;
        bioassert(reg->hasOneBlock(), "Connected component region did not have 1 block");

        // Get the ops for this connected component
        mlir::Block& block = reg->front();
        mlir::Block::OpListType& ops = block.getOperations();

        // Get the block for main
        const auto mainEnd = mainBlock->end();
        mlir::Block::OpListType& mainOps = mainBlock->getOperations();

        // Splice the ops for this component into the end of main
        mainOps.splice(mainEnd, ops);

        _opBuilder.setInsertionPointToEnd(mainBlock);
        return;
    }

    llvm::SmallVector<mlir::Value> results;
    buildCrossProductCascade(components, mainBlock, results);

    size_t resultIndex = 0;
    for (const TranslatedComponent& component : components) {
        for (const VariableDependency* var : component._vars) {
            registerValue(var, results[resultIndex]);
            resultIndex++;
        }
    }

    _opBuilder.setInsertionPointToEnd(mainBlock);
}

// A chunk holds the rows of the loop whose body binds it, so a column bound in an
// enclosing block holds a different row set. Stricter than dominance on purpose: such
// a value does dominate here, it is just the wrong rows.
bool DBProgramGenerator::isRowAlignedHere(mlir::Value column) const {
    mlir::Operation* const definingOp = column.getDefiningOp();
    mlir::Block* const definingBlock = definingOp
        ? definingOp->getBlock()
        : mlir::cast<mlir::BlockArgument>(column).getOwner();

    return definingBlock == _opBuilder.getInsertionBlock();
}

void DBProgramGenerator::collectInFlightColumns(InFlightColumns& inFlight) {
    for (auto& [var, values] : _part._varMap) {
        const mlir::Value column = values.back();
        if (!isRowAlignedHere(column)) {
            continue;
        }

        // A constant stands for every row rather than holding rows of its own, so an op
        // over the row set has no row of it to cut or to cross
        if (yieldsConstantColumn(column)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._variables.push_back(var);
    }

    for (auto& [var, column] : _part._edgeTypeMap) {
        if (!isRowAlignedHere(column)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._edgeTypeVariables.push_back(var);
    }

    // A column an earlier CALL yielded is in flight too: a later op taking the whole row
    // set must take it along, or the rows it holds would stop matching the ones beside it.
    for (size_t yieldedIndex = 0; yieldedIndex < _part._yieldedColumns.size(); yieldedIndex++) {
        const mlir::Value column = _part._yieldedColumns[yieldedIndex]._column;
        if (!isRowAlignedHere(column)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._yieldedIndices.push_back(yieldedIndex);
    }
}

void DBProgramGenerator::rebindInFlightColumns(mlir::Operation::result_range results,
                                               size_t firstResult,
                                               const InFlightColumns& inFlight) {
    size_t resultIndex = firstResult;

    for (const VariableDependency* variable : inFlight._variables) {
        registerValue(variable, results[resultIndex]);
        resultIndex++;
    }

    for (const VariableDependency* variable : inFlight._edgeTypeVariables) {
        _part._edgeTypeMap[variable] = results[resultIndex];
        resultIndex++;
    }

    for (const size_t yieldedIndex : inFlight._yieldedIndices) {
        _part._yieldedColumns[yieldedIndex]._column = results[resultIndex];
        resultIndex++;
    }
}

mlir::Value DBProgramGenerator::findYieldedColumn(const VarDecl* decl) const {
    if (!decl) {
        return mlir::Value {};
    }

    for (const YieldedColumn& yielded : _part._yieldedColumns) {
        if (yielded._decl == decl) {
            return yielded._column;
        }
    }

    return mlir::Value();
}

void DBProgramGenerator::filterAllColumns(mlir::Value predicate) {
    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    // A scope of constants alone carries no rows for the filter to cut, so the predicate is
    // laid out over the single row those constants are, and cuts that one instead
    if (inFlight._columns.empty()) {
        filterConstantScope(predicate);
        return;
    }

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto filterOp = _opBuilder.create<mlir::db::FilterOp>(loc, resultTypes, predicate, inFlight._columns);

    rebindInFlightColumns(filterOp.getResults(), /*firstResult=*/0, inFlight);
}

void DBProgramGenerator::filterConstantScope(mlir::Value predicate) {
    llvm::SmallVector<const VariableDependency*> constantVars;
    for (auto& [var, values] : _part._varMap) {
        if (yieldsConstantColumn(values.back())) {
            constantVars.push_back(var);
        }
    }

    if (constantVars.empty()) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Value noDriver;

    const mlir::Value mask =
        _opBuilder.create<mlir::db::BroadcastConstant>(loc, noneType, predicate, noDriver).getResult();

    llvm::SmallVector<mlir::Value> columnsToFilter;
    llvm::SmallVector<mlir::Type> resultTypes;
    for (const VariableDependency* var : constantVars) {
        const mlir::Value column = _part._varMap.at(var).back();
        const mlir::Value laidOut =
            _opBuilder.create<mlir::db::BroadcastConstant>(loc, noneType, column, noDriver).getResult();

        columnsToFilter.push_back(laidOut);
        resultTypes.push_back(laidOut.getType());
    }

    auto filterOp = _opBuilder.create<mlir::db::FilterOp>(loc, resultTypes, mask, columnsToFilter);

    for (size_t index = 0; index < constantVars.size(); index++) {
        registerValue(constantVars[index], filterOp.getResult(index));
    }
}

void DBProgramGenerator::addMergeFilter(const VariableDependency* mergeVar,
                                        std::vector<const VariableDependency*>& carriedSet) {
    const VariableDependency* fstMergeSource = nullptr;
    const VariableDependency* sndMergeSource = nullptr;
    for (const DependencyEdge* inEdge : mergeVar->incoming()) {
        if (!inEdge->isMetaEdge()) {
            continue;
        }
        if (!fstMergeSource) {
            fstMergeSource = inEdge->src();
        } else {
            sndMergeSource = inEdge->src();
        }
    }
    bioassert(fstMergeSource && sndMergeSource, "MERGE target without two sources");

    const mlir::Location uloc = _opBuilder.getUnknownLoc();
    const mlir::Value fstSourceCol = _part._varMap.at(fstMergeSource).back();
    const mlir::Value sndSourceCol = _part._varMap.at(sndMergeSource).back();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    // Create an EQ op to keep only rows where both sources are the same
    auto eq =
        _opBuilder.create<mlir::db::EqOp>(uloc, boolType, fstSourceCol, sndSourceCol);
    const mlir::Value eqRes = eq.getResult();

    // Register mergeVar's initial value (first source, arbitrary) so filterAllColumns
    // picks it up along with the rest of the part's variables.
    registerValue(mergeVar, fstSourceCol);
    filterAllColumns(eqRes);
    carriedSet.push_back(mergeVar);
}

void DBProgramGenerator::resolveEdgeIdentities() {
    const mlir::Location uloc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType =
        allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    // Join Cypher edge variables which had multiple VariableDependency*s in @ref _vdg
    for (const auto& [name, vars] : _vdg.edgeIdentities()) {
        const bool needsFilter = vars.size() > 1;
        if (!needsFilter) {
            continue;
        }

        mlir::Value predicate;
        for (size_t index = 0; index + 1 < vars.size(); index++) {
            const VariableDependency* fstVar = vars[index];
            const VariableDependency* sndVar = vars[index + 1];
            const mlir::Value fstCol = findVarOrThrow(_part._varMap, fstVar);
            const mlir::Value sndCol = findVarOrThrow(_part._varMap, sndVar);

            auto eqOp = _opBuilder.create<mlir::db::EqOp>(uloc, boolType, fstCol, sndCol);
            const mlir::Value eq = eqOp.getResult();

            if (!predicate) {
                predicate = eq;
            } else {
                auto andOp = _opBuilder.create<mlir::db::AndOp>(uloc, boolType, predicate, eq);
                predicate = andOp.getResult();
            }
        }

        filterAllColumns(predicate);
    }
}

void DBProgramGenerator::throwOnRematchedBoundEdge() const {
    const VariableDependencyGraph::EdgeIdentityMap& identities = _vdg.edgeIdentities();

    for (const VariableDependency* var : _vdg.boundVars()) {
        if (identities.contains(var->getDecl())) {
            throw TuringException(fmt::format("Matching the edge variable '{}' again after a "
                                              "WITH is not yet supported.",
                                              var->getName()));
        }
    }
}

bool DBProgramGenerator::holdsColumn(const VariableDependency* var) const {
    const auto findIt = _part._varMap.find(var);

    return findIt != _part._varMap.end() && !findIt->second.empty();
}

void DBProgramGenerator::throwOnUnboundPatternVariable() const {
    for (const VariableDependency& var : _vdg.vars()) {
        if (holdsColumn(&var)) {
            continue;
        }

        throw TuringException(fmt::format("Reaching the pattern variable '{}' from the rest "
                                          "of the query is not yet supported.",
                                          var.getName()));
    }
}

void DBProgramGenerator::throwOnDroppedUnwindSeed() const {
    for (const auto& [var, unwind] : _vdg.unwindSources()) {
        const VarDecl* const decl = var->getDecl();
        const bool namesAPatternNode = decl && decl->getType() == EvaluatedType::NodePattern;

        if (!namesAPatternNode || _part._seededVars.contains(var)) {
            continue;
        }

        throw TuringException(fmt::format("Unwinding node IDs into the pattern variable '{}' is "
                                          "not yet supported where the pattern reaches it from "
                                          "elsewhere.",
                                          var->getName()));
    }
}

void DBProgramGenerator::extendBoundDataflow(DefinedVars& defined,
                                             std::vector<const VariableDependency*>& dataflowVars) {
    const VariableDependencyGraph::BoundVars& bound = _vdg.boundVars();

    // Every column the barrier published already holds its rows, so no scan opens a
    // dataflow here: the hops of this part extend that one, carrying those columns along.
    // A constant has no rows to carry and rides along untouched
    std::vector<const VariableDependency*> carriedSet;
    for (const VariableDependency* var : bound) {
        defined.insert(var);

        if (!yieldsConstantColumn(_part._varMap.at(var).back())) {
            carriedSet.push_back(var);
            dataflowVars.push_back(var);
        }
    }

    for (const VariableDependency* var : bound) {
        applyConstraints(var);
    }

    for (const VariableDependency* var : bound) {
        expandComponent(var, defined, carriedSet, dataflowVars);
    }

    closeBoundJoins(carriedSet, dataflowVars);
}

void DBProgramGenerator::closeBoundMerges() {
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    for (const VariableDependency* var : _vdg.boundVars()) {
        for (const DependencyEdge* inEdge : var->incoming()) {
            if (!inEdge->isMetaEdge()) {
                continue;
            }

            const mlir::Value landed = findVarOrThrow(_part._varMap, inEdge->src());
            const mlir::Value bound = findVarOrThrow(_part._varMap, var);

            auto eq = _opBuilder.create<mlir::db::EqOp>(uloc, boolType, landed, bound);
            filterAllColumns(eq.getResult());
        }
    }
}

void DBProgramGenerator::resolveYieldedIdentities() {
    struct YieldedIdentity {
        const VariableDependency* _variable {nullptr};
        size_t _yieldedIndex {0};
    };

    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();

    llvm::SmallVector<YieldedIdentity> identities;
    for (size_t yieldedIndex = 0; yieldedIndex < _part._yieldedColumns.size(); yieldedIndex++) {
        const VarDecl* yieldedDecl = _part._yieldedColumns[yieldedIndex]._decl;
        if (!yieldedDecl) {
            continue;
        }

        for (const auto& [var, values] : _part._varMap) {
            if (var->getDecl() != yieldedDecl) {
                continue;
            }

            identities.push_back({var, yieldedIndex});
        }

        const auto edgeIt = edgeIdentities.find(yieldedDecl);
        const bool yieldsAPatternEdge = edgeIt != edgeIdentities.end() && !edgeIt->second.empty();
        if (yieldsAPatternEdge) {
            identities.push_back({edgeIt->second.front(), yieldedIndex});
        }
    }

    if (identities.empty()) {
        return;
    }

    const mlir::Location uloc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    for (const YieldedIdentity& identity : identities) {
        const mlir::Value patternColumn = _part._varMap.at(identity._variable).back();
        const mlir::Value yieldedColumn = _part._yieldedColumns[identity._yieldedIndex]._column;

        const bool patternAligned = isRowAlignedHere(patternColumn);
        const bool yieldedAligned = isRowAlignedHere(yieldedColumn);
        bioassert(patternAligned && yieldedAligned,
                  "Yielded variable {} was not paired with its pattern",
                  identity._variable->getName());

        auto eqOp = _opBuilder.create<mlir::db::EqOp>(uloc, boolType, patternColumn, yieldedColumn);

        filterAllColumns(eqOp.getResult());
    }
}

void DBProgramGenerator::closeBoundJoins(std::vector<const VariableDependency*>& carriedSet,
                                         std::vector<const VariableDependency*>& dataflowVars) {
    // The walk orients a hop by the one end of it that is not yet bound, so it leaves
    // behind the hops whose ends were both bound already - a pattern reaching back to a
    // variable the barrier published, which constrains the rows rather than fanning them out
    for (const VariableDependency& var : _vdg.vars()) {
        if (holdsColumn(&var)) {
            continue;
        }

        const VariableDependency::Edges& incoming = var.incoming();
        const VariableDependency::Edges& outgoing = var.outgoing();

        const auto edgeProducerIt = std::ranges::find_if(incoming, producesEdgeVar);
        const auto nodeProducerIt = std::ranges::find_if(outgoing, producesNodeVar);

        const bool isPatternEdge = edgeProducerIt != end(incoming) && nodeProducerIt != end(outgoing);
        if (!isPatternEdge) {
            continue;
        }

        const VariableDependency* target = (*nodeProducerIt)->tgt();

        // A hop left behind with an end still open is not a join but a pattern of its own,
        // which this walk does not reach: only both ends holding a column already make the
        // hop a constraint on the rows they were bound over
        const bool closesAJoin = holdsColumn((*edgeProducerIt)->src()) && holdsColumn(target);
        if (!closesAJoin) {
            continue;
        }

        closeBoundJoin(*edgeProducerIt, &var, target, carriedSet, dataflowVars);
    }
}

void DBProgramGenerator::closeBoundJoin(const DependencyEdge* edgeProducer,
                                        const VariableDependency* edge,
                                        const VariableDependency* target,
                                        std::vector<const VariableDependency*>& carriedSet,
                                        std::vector<const VariableDependency*>& dataflowVars) {
    const VariableDependency* source = edgeProducer->src();

    mlir::Value landed;
    const EdgeMetadata::EdgeType direction = edgeProducer->data().type();

    switch (direction) {
        case EdgeMetadata::EdgeType::GET_OUT_EDGES:
            landed = addJoiningEdgeTraversal<mlir::db::GetOutEdges>(source, edge, target, carriedSet);
        break;

        case EdgeMetadata::EdgeType::GET_IN_EDGES:
            landed = addJoiningEdgeTraversal<mlir::db::GetInEdges>(source, edge, target, carriedSet);
        break;

        case EdgeMetadata::EdgeType::GET_EDGES:
            landed = addJoiningEdgeTraversal<mlir::db::GetEdges>(source, edge, target, carriedSet);
        break;

        default:
            throw FatalException(fmt::format("Attempted to close a join over {}",
                                             EdgeTypeName::value(direction)));
        break;
    }

    const mlir::Value bound = findVarOrThrow(_part._varMap, target);
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    auto eq = _opBuilder.create<mlir::db::EqOp>(_opBuilder.getUnknownLoc(), boolType, landed, bound);
    filterAllColumns(eq.getResult());

    applyConstraints(edge);

    carriedSet.push_back(edge);

    // The walk lists an edge as soon as it reaches it, before any hop has given it a
    // column, so one whose ends were both bound is already there with nothing behind it
    if (std::ranges::find(dataflowVars, edge) == dataflowVars.end()) {
        dataflowVars.push_back(edge);
    }
}

void DBProgramGenerator::translateComponent(const VariableDependency* root,
                                            DefinedVars& defined,
                                            std::vector<const VariableDependency*>& outVars) {
    // A root's dataflow opens with whatever already holds its rows: the column a CALL bound
    // it to, the literal list an UNWIND binds it to, or - a pattern variable nothing has
    // bound yet - a scan of the graph's nodes.
    const VariableDependencyGraph::UnwindSourceMap& unwindSources = _vdg.unwindSources();
    const auto unwindIt = unwindSources.find(root);
    const mlir::Value yieldedColumn = findYieldedColumn(root->getDecl());

    if (yieldedColumn) {
        addYieldedColumn(root, yieldedColumn);
    } else if (unwindIt != unwindSources.end()) {
        const VarDecl* const decl = root->getDecl();
        const bool seedsAPatternNode = decl && decl->getType() == EvaluatedType::NodePattern;

        // A pattern naming the unwound variable makes its list a set of node IDs rather
        // than a column of values, so the nodes themselves open the dataflow.
        if (seedsAPatternNode) {
            addConstScanNodes(root, unwindIt->second);
        } else {
            addUnwindConst(root, unwindIt->second);
        }
    } else {
        addScanNodes(root);
    }

    applyConstraints(root);

    // Forms the "carried set" for this connected component
    std::vector<const VariableDependency*> carriedSet;

    expandComponent(root, defined, carriedSet, outVars);
}

void DBProgramGenerator::expandComponent(const VariableDependency* root,
                                         DefinedVars& defined,
                                         std::vector<const VariableDependency*>& carriedSet,
                                         std::vector<const VariableDependency*>& outVars) {
    struct Frame {
        const VariableDependency* _var {nullptr};
        const DependencyEdge* _predEdge {nullptr};
        const DependencyEdge* _predPredEdge {nullptr};
    };

    // Makes var an output on first encounter
    const auto markDefined = [&](const VariableDependency* var) {
        if (defined.insert(var).second) {
            outVars.push_back(var);
        }
    };

    markDefined(root);

    std::vector<Frame> stack;

    // DFS from this root
    stack.emplace_back(root, nullptr);
    while (!stack.empty()) {
        const auto [var, pred, predPred] = stack.back();
        stack.pop_back();

        const auto seenOrMeta = [&defined](const DependencyEdge* e) {
            return !e->isMetaEdge() || defined.contains(e->src());
        };
        const bool canTraverse = std::ranges::all_of(var->incoming(), seenOrMeta);

        // If we cannot traverse now, we will find another path to this node
        if (!canTraverse) {
            continue;
        }

        // A merge edge joins two dataflows instead of traversing the graph, so it never
        // is half of a (source, edge, target) triple: it closes the chain it lands on,
        // and whatever leaves its target opens a new one.
        const bool predTraverses = pred && !pred->isMetaEdge();

        // Have we found a (source, edge, target) triple yet on this traversal?
        const bool haveTriple = predTraverses && predPred;

        for (const DependencyEdge* e : var->edges()) {
            const VariableDependency* other = e->src() == var ? e->tgt() : e->src();
            if (defined.contains(other)) {
                continue;
            }

            if (haveTriple || !predTraverses) {
                // We have discovered a full (src, edge, tgt) triple, or @ref pred is a
                // merge edge that cannot open one: either way the next elements on the
                // stack only have (src, edge) and await tgt
                stack.emplace_back(other, e, nullptr);
            } else {
                // We have not yet discovered a full (src, edge, tgt) triple, but the
                // next elements on stack will have such a triple (with @ref pred)
                stack.emplace_back(other, e, pred);
            }
        }

        // Only translate when we have a full triple
        if (!haveTriple) {
            if (pred && pred->isMetaEdge()) {
                addMergeFilter(var, carriedSet);
                applyConstraints(var);
            }

            markDefined(var);
            continue;
        }

        // The order we encountered the nodes may not be source, edge, target, it may
        // be target, edge, source. Determine a definitive order, irrespective of
        // traversal
        const DependencyEdge* edgeVarProd = producesEdgeVar(pred) ? pred : predPred;
        const DependencyEdge* nodeVarProd = producesNodeVar(pred) ? pred : predPred;
        bioassert(producesEdgeVar(edgeVarProd), "No edge producer");
        bioassert(producesNodeVar(nodeVarProd), "No node producer");

        // Only one of either source or target should be defined. Determine which end
        // of the triple is defined
        const bool edgeSrcDefined = defined.contains(edgeVarProd->src());
        const bool nodeTgtDefined = defined.contains(nodeVarProd->tgt());
        bioassert(edgeSrcDefined ^ nodeTgtDefined, "Ambiguous definition");
        bioassert(edgeSrcDefined || nodeTgtDefined, "No defined start");

        VariableDependency* src = nullptr;
        VariableDependency* edge = nullptr;
        VariableDependency* tgt = nullptr;

        // Orientate the operation such that the source operand is defined
        if (edgeSrcDefined) {
            src = edgeVarProd->src();
            edge = edgeVarProd->tgt();
            tgt = nodeVarProd->tgt();
        } else /* (nodeTgtDefined) */ {
            src = nodeVarProd->tgt();
            edge = nodeVarProd->src();
            tgt = edgeVarProd->src();
        }

        // We may walk an edge backwards compared to the cypher pattern. In such a
        // case we emit the opposite traversal.
        const EdgeMetadata::EdgeType prodType = edgeVarProd->data().type();
        const EdgeMetadata::EdgeType logicalDir =
            edgeSrcDefined ? prodType : reverseEdge(prodType);

        switch (logicalDir) {
            case EdgeMetadata::EdgeType::GET_OUT_EDGES:
                addGetOutEdges(src, edge, tgt, carriedSet);
            break;

            case EdgeMetadata::EdgeType::GET_IN_EDGES:
                addGetInEdges(src, edge, tgt, carriedSet);
            break;

            case EdgeMetadata::EdgeType::MERGE:
                throw TuringException("MERGE edges not yet supported.");
            break;

            case EdgeMetadata::EdgeType::GET_EDGES:
                addGetEdges(src, edge, tgt, carriedSet);
            break;

            case EdgeMetadata::EdgeType::GET_EDGE_TGT:
            case EdgeMetadata::EdgeType::GET_EDGE_SRC:
                throw FatalException(fmt::format("Attempted to translate {}",
                                                 EdgeTypeName::value(logicalDir)));
            break;

            case EdgeMetadata::EdgeType::_SIZE:
                throw FatalException("Attempted to translate invalid edge.");
            break;
        }

        markDefined(src);
        markDefined(edge);
        markDefined(tgt);

        applyConstraints(edge);
        applyConstraints(tgt);

        carriedSet.push_back(src);
        carriedSet.push_back(edge);
        carriedSet.push_back(tgt);
    }
}

void DBProgramGenerator::takeMainDataflow(mlir::Block* mainBlock, TranslatedComponent& component) {
    for (const VariableDependency* var : component._vars) {
        bioassert(holdsColumn(var), "Main dataflow var {} not registered", var->getName());
        component._columns.push_back(_part._varMap.at(var).back());
    }

    component._region = std::make_unique<mlir::Region>();
    mlir::Block* const scratch = new mlir::Block(); // Region destructor frees scratch
    component._region->push_back(scratch);

    const auto bindsAConstant = [](mlir::Value result) {
        return yieldsConstantColumn(result);
    };

    // A constant is left above the product: it holds one value standing for every row, so
    // neither factor crosses it and both read the one binding
    for (mlir::Operation& operation : llvm::make_early_inc_range(*mainBlock)) {
        const bool bindsColumns = operation.getNumResults() > 0;
        const bool bindsConstantsOnly = bindsColumns
                                        && llvm::all_of(operation.getResults(), bindsAConstant);

        if (bindsConstantsOnly) {
            continue;
        }

        operation.moveBefore(scratch, scratch->end());
    }
}

void DBProgramGenerator::buildCrossProductCascade(std::vector<TranslatedComponent>& components,
                                                  mlir::Block* targetBlock,
                                                  llvm::SmallVectorImpl<mlir::Value>& results) {
    const size_t numComponents = components.size();
    bioassert(numComponents >= 2, "Cross product cascade needs at least two components");

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    mlir::Block* currentTarget = targetBlock;

    mlir::Block* pendingYieldBlock = nullptr;

    // Fold over all components, applying a CrossProduct between them
    // comp1 x (comp2 x (comp3 x ...))
    for (size_t i = 0; i + 1 < numComponents; i++) {
        // Component i crossed with the cross of all subsequent component js
        // The result of this cross is result of i x i + 1 x i + 2 x ... x j
        llvm::SmallVector<mlir::Type> resultTypes;
        for (size_t j = i; j < numComponents; j++) {
            for (const mlir::Value column : components[j]._columns) {
                resultTypes.push_back(column.getType());
            }
        }

        // Starts as main block, then updated to the RHS of the previous factor
        _opBuilder.setInsertionPointToEnd(currentTarget);
        // Create a cross for i x j
        auto crossProduct = _opBuilder.create<mlir::db::CrossProduct>(loc, resultTypes);

        mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();
        mlir::Block* const rightBlock = &crossProduct.getRightFactor().front();

        // Component i occupies the LHS of this cross prod
        moveComponentToFactor(components[i], leftBlock);

        const mlir::ResultRange crossResults = crossProduct.getResults();
        // There is no pending yield iff we have a single cross product between 2
        // components
        if (pendingYieldBlock) {
            _opBuilder.setInsertionPointToEnd(pendingYieldBlock);
            _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {crossResults});
        } else {
            results.assign(crossResults.begin(), crossResults.end());
        }

        // If this is the final cross prod, move this component to the right hand side
        const bool lastPair = i + 2 == numComponents;
        if (lastPair) {
            moveComponentToFactor(components[i + 1], rightBlock);
        } else {
            // Otherwise the next component will be assigned to the right block
            currentTarget = rightBlock;
            pendingYieldBlock = rightBlock;
        }
    }
}

void DBProgramGenerator::moveComponentToFactor(TranslatedComponent& component,
                                               mlir::Block* factorBlock) {
    // Insertion point into the factor
    auto factorEnd = factorBlock->end();

    auto& compReg = component._region;
    mlir::Block& compBlock = compReg->front();
    // Operations to insert
    mlir::Block::OpListType& compOps = compBlock.getOperations();

    factorBlock->getOperations().splice(factorEnd, compOps);

    _opBuilder.setInsertionPointToEnd(factorBlock);
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {component._columns});
}

void DBProgramGenerator::generateLeadingYields(std::span<Stmt* const> stmts) {
    if (_vdg.empty()) {
        return;
    }

    // A pattern variable a barrier published is a dataflow this part extends, so what a
    // statement binds is paired with its rows after the traversal rather than driving it.
    // A published value is no such dataflow - it is read, the way an UNWIND reads the list
    // it spreads - and the rows a statement turns it into can drive.
    const auto bindsAValue = [](const VariableDependency* bound) {
        const VarDecl* decl = bound->getDecl();
        const EvaluatedType type = decl ? decl->getType() : EvaluatedType::Invalid;

        return type != EvaluatedType::NodePattern && type != EvaluatedType::EdgePattern;
    };

    if (!std::ranges::all_of(_vdg.boundVars(), bindsAValue)) {
        return;
    }

    llvm::SmallVector<const Stmt*> leading;
    for (const Stmt* stmt : stmts) {
        const Stmt::Kind kind = stmt->getKind();

        if (kind == Stmt::Kind::MATCH) {
            break;
        } else if (kind == Stmt::Kind::CALL || kind == Stmt::Kind::VECTOR_SEARCH) {
            leading.push_back(stmt);
        } else if (kind == Stmt::Kind::UNWIND) {
            // A literal UNWIND is a dataflow of its own that the dependency graph already
            // roots, and seeds the pattern node it names from there.
            const UnwindStmt* unwindStmt = static_cast<const UnwindStmt*>(stmt);
            if (!unwindStmt->unwindsLiteral()) {
                leading.push_back(stmt);
            }
        }
    }

    if (leading.empty()) {
        return;
    }

    llvm::SmallVector<const VariableDependency*> componentRoots;
    collectComponentRoots(componentRoots);

    // A published value opens no traversal: it rides the rows as a column of its own, so
    // it is no island for the driven component to be crossed with.
    const auto opensNoTraversal = [this](const VariableDependency* root) {
        return llvm::is_contained(_vdg.boundVars(), root);
    };

    llvm::erase_if(componentRoots, opensNoTraversal);

    llvm::SmallVector<const VarDecl*> yieldedVariables;
    for (const Stmt* stmt : leading) {
        collectStatementVariables(stmt, yieldedVariables);
    }

    // Any variable they bound can open the component, not only the one the graph lists
    // first: a pattern that reaches the yielded variable from its other end is walked
    // backwards from it, which is the traversal a reversed edge already emits.
    const VariableDependency* drivenRoot = nullptr;
    for (const VariableDependency& var : _vdg.vars()) {
        const bool bound = llvm::is_contained(yieldedVariables, var.getDecl());
        const bool canDrive = bound && isValidRoot(var);

        if (canDrive) {
            drivenRoot = &var;
            break;
        }
    }

    if (!drivenRoot) {
        return;
    }

    // Crossing a second component with the driven one leaves anything else the statements
    // yielded inside a factor of that product, out of reach of the clause that reads it, so
    // only the root coming out of them alone makes the island safe to cross.
    const bool hasIslands = componentRoots.size() > 1;
    const bool yieldsTheRootAlone = yieldedVariables.size() == 1
                                    && yieldedVariables.front() == drivenRoot->getDecl();

    if (hasIslands && !yieldsTheRootAlone) {
        return;
    }

    for (const Stmt* stmt : leading) {
        const Stmt::Kind kind = stmt->getKind();

        if (kind == Stmt::Kind::CALL) {
            generateCall(static_cast<const CallStmt*>(stmt));
        } else if (kind == Stmt::Kind::VECTOR_SEARCH) {
            generateVectorSearch(static_cast<const VectorSearchStmt*>(stmt));
        } else {
            generateUnwind(static_cast<const UnwindStmt*>(stmt));
        }
    }

    _part._drivenRoot = drivenRoot;
}

void DBProgramGenerator::generateStatementOperations(std::span<Stmt* const> stmts) {
    // In statement order, so a call or a search reading what an earlier one yielded sees
    // it, a MATCH's constraints and WHERE reading a yielded or unwound value are generated
    // once the statement that binds it has run, and an UNWIND expands the rows the
    // statements ahead of it left in flight.
    bool matchSeen = false;
    for (const Stmt* stmt : stmts) {
        const Stmt::Kind kind = stmt->getKind();
        const bool drivesTheTraversal = _part._drivenRoot && !matchSeen;

        if (kind == Stmt::Kind::MATCH) {
            matchSeen = true;

            const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
            generateMatchConstraints(matchStmt);
            generateMatchFilter(matchStmt);
            generateMatchOrderBy(matchStmt);
            generateMatchWindow(matchStmt);
        } else if (kind == Stmt::Kind::CALL && !drivesTheTraversal) {
            generateCall(static_cast<const CallStmt*>(stmt));
        } else if (kind == Stmt::Kind::VECTOR_SEARCH && !drivesTheTraversal) {
            generateVectorSearch(static_cast<const VectorSearchStmt*>(stmt));
        } else if (kind == Stmt::Kind::UNWIND && !drivesTheTraversal) {
            const UnwindStmt* unwindStmt = static_cast<const UnwindStmt*>(stmt);

            // A literal UNWIND opened a dataflow of its own during the traversal, which
            // the cross product has already paired with the rows beside it.
            if (!unwindStmt->unwindsLiteral()) {
                generateUnwind(unwindStmt);
            }
        }
    }
}

void DBProgramGenerator::generateMatchFilter(const MatchStmt* matchStmt) {
    const Pattern* pattern = matchStmt->getPattern();
    const WhereClause* where = pattern->getWhere();
    if (!where) {
        return;
    }

    std::vector<const Expr*> conjuncts;
    flattenConjuncts(where->getExpr(), conjuncts);

    applyPredicateFilters(conjuncts);
}

void DBProgramGenerator::generateMatchOrderBy(const MatchStmt* matchStmt) {
    if (!matchStmt->hasOrderBy()) {
        return;
    }

    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    if (inFlight._columns.empty()) {
        return;
    }

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    llvm::SmallVector<mlir::Value> sorted(inFlight._columns.begin(), inFlight._columns.end());
    llvm::SmallVector<int64_t> keyColumns;
    llvm::SmallVector<bool> keyAscending;

    for (const OrderByItem* item : matchStmt->getOrderBy()->getItems()) {
        const Expr* keyExpr = item->getExpr();

        // A constant key holds the same value in every row, so it changes no order
        if (!keyExpr->isDynamic()) {
            continue;
        }

        const mlir::Value keyColumn = getOrTranslateExprColumn(variableColumns, keyExpr);
        if (yieldsConstantColumn(keyColumn)) {
            continue;
        }

        // A key the match does not carry - the n.name of MATCH (n) ORDER BY n.name - is
        // handed to the sort as one more column, so that it moves with the row it belongs
        // to; its result is then left unread, which is what keeps it out of the rows
        auto sortedKey = std::find(sorted.begin(), sorted.end(), keyColumn);
        if (sortedKey == sorted.end()) {
            sorted.push_back(keyColumn);
            sortedKey = std::prev(sorted.end());
        }

        keyColumns.push_back(static_cast<int64_t>(std::distance(sorted.begin(), sortedKey)));
        keyAscending.push_back(item->getType() == OrderByType::ASC);
    }

    if (keyColumns.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Type> sortResultTypes;
    for (const mlir::Value column : sorted) {
        sortResultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto sortOp = _opBuilder.create<mlir::db::Sort>(loc,
                                                    sortResultTypes,
                                                    mlir::ValueRange {sorted},
                                                    keyColumns,
                                                    keyAscending);

    rebindInFlightColumns(sortOp.getResults(), /*firstResult=*/0, inFlight);
}

void DBProgramGenerator::generateMatchWindow(const MatchStmt* matchStmt) {
    if (matchStmt->hasSkip()) {
        const int64_t skipValue = evaluateConstantInteger(matchStmt->getSkip()->getExpr());

        if (skipValue < 0) {
            throw TuringException("SKIP expression must be a non-negative integer");
        }

        cutAllColumns<mlir::db::Skip>(static_cast<uint64_t>(skipValue));
    }

    if (matchStmt->hasLimit()) {
        const int64_t limitValue = evaluateConstantInteger(matchStmt->getLimit()->getExpr());

        if (limitValue < 0) {
            throw TuringException("LIMIT expression must be a non-negative integer");
        }

        cutAllColumns<mlir::db::Limit>(static_cast<uint64_t>(limitValue));
    }
}

template <typename CutOp>
void DBProgramGenerator::cutAllColumns(uint64_t count) {
    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    if (inFlight._columns.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto cutOp = _opBuilder.create<CutOp>(loc, resultTypes, inFlight._columns, count);

    rebindInFlightColumns(cutOp.getResults(), /*firstResult=*/0, inFlight);
}

void DBProgramGenerator::generateCall(const CallStmt* callStmt) {
    const FunctionInvocationExpr* funcExpr = callStmt->getFunc();
    if (!funcExpr) {
        throw TuringException("CALL statement has no procedure invocation.");
    }

    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
    if (!invocation) {
        throw TuringException("CALL statement has no function invocation.");
    }

    const FunctionSignature* signature = invocation->getSignature();
    if (!signature) {
        throw TuringException("CALL statement has an unresolved procedure name.");
    }

    const std::string_view procedureName = signature->getFullName();

    // The yielded return values name the columns the call produces.
    const YieldClause* yield = callStmt->getYield();
    const YieldItems* yieldItems = yield ? yield->getItems() : nullptr;

    llvm::SmallVector<mlir::Attribute> yieldedNames;
    llvm::SmallVector<YieldedColumn> yielded;
    if (yieldItems && !yieldItems->getItems().empty()) {
        for (const SymbolExpr* item : *yieldItems) {
            const Symbol* symbol = item->getSymbol();

            // The original name is the procedure's own name for the return value, which
            // is what the call op names; the symbol's name is what the query calls it,
            // which is the alias when the YIELD renamed it.
            yieldedNames.push_back(_opBuilder.getStringAttr(symbol->getOriginalName()));
            yielded.push_back({item->getDecl(), symbol->getName(), mlir::Value {}, /*isResult=*/true});
        }
    } else {
        // A call naming no return value produces every one the procedure declares, under
        // the procedure's own names - and none at all for a procedure declaring none,
        // which is called for what it does rather than for rows. Only a standalone call
        // gets here: the analyzer requires a YIELD of any call inside a query.
        for (const FunctionReturnType& returnType : signature->returnTypes()) {
            yieldedNames.push_back(_opBuilder.getStringAttr(returnType.getName()));
            yielded.push_back({nullptr, returnType.getName(), mlir::Value {}, /*isResult=*/true});
        }
    }

    // One argument column per declared procedure argument, in the order written: a
    // variable resolves to its current column, a literal to a constant.
    llvm::SmallVector<mlir::Value> inputs;
    const ExprChain* arguments = invocation->getArguments();
    if (arguments) {
        for (const Expr* argument : *arguments) {
            translateExpr(argument);
            inputs.push_back(_part._exprMap.at(argument));
        }
    }

    // Everything already in flight rides through the carry set, so the projection can
    // still read it after the call: the procedure need not emit one row per row it was
    // given, and the call replicates a carried row once per row it emitted for it.
    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    // A call reading none of those rows produces the same rows for every one of them, which
    // is their cartesian product rather than anything a carry set can express. Taking no
    // argument is one way to read none of them; taking only constant ones is another.
    if (!inFlight._columns.empty() && !readsAnyColumn(inputs, inFlight._columns)) {
        generateCrossedCall(procedureName, yieldedNames, yielded, inputs, inFlight);
        generateYieldFilter(yieldItems);
        return;
    }

    // The yielded columns' value types come from the procedure's declared return types,
    // resolved during lowering, so they are left unresolved here - as a property fetch's
    // value column is. A carried column keeps its own type: the call replicates its rows,
    // it never retypes them.
    llvm::SmallVector<mlir::Type> resultTypes;
    for (size_t yieldIndex = 0; yieldIndex < yieldedNames.size(); yieldIndex++) {
        resultTypes.push_back(mlir::db::ColumnType::get(_mlirCtxt));
    }

    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto callOp = _opBuilder.create<mlir::db::CallProcedure>(loc,
                                                            resultTypes,
                                                            _opBuilder.getStringAttr(procedureName),
                                                            mlir::ValueRange {inputs},
                                                            mlir::ValueRange {inFlight._columns},
                                                            _opBuilder.getArrayAttr(yieldedNames));

    // The yielded columns are new variables of the query; the carried ones are the rows
    // already in flight, now aligned with what the procedure emitted.
    const mlir::Operation::result_range results = callOp.getResults();
    for (size_t yieldIndex = 0; yieldIndex < yielded.size(); yieldIndex++) {
        yielded[yieldIndex]._column = results[yieldIndex];
        _part._yieldedColumns.push_back(yielded[yieldIndex]);
    }

    rebindInFlightColumns(results, yielded.size(), inFlight);

    generateYieldFilter(yieldItems);
}

void DBProgramGenerator::generateCSVLoads(std::span<Stmt* const> stmts) {
    for (const Stmt* stmt : stmts) {
        if (stmt->getKind() == Stmt::Kind::LOAD_CSV) {
            generateLoadCSV(static_cast<const LoadCSVStmt*>(stmt));
        }
    }
}

void DBProgramGenerator::generateLoadCSV(const LoadCSVStmt* loadCSVStmt) {
    const std::span<const LoadCSVStmt::Field> fields = loadCSVStmt->fields();

    const auto positionAttr = [this](size_t index) -> mlir::Attribute {
        return mlir::IntegerAttr::get(_opBuilder.getIntegerType(64, /*isSigned=*/false), index);
    };

    llvm::SmallVector<mlir::Attribute> fieldAttrs;
    llvm::SmallVector<const VarDecl*> fieldDecls;
    for (const LoadCSVStmt::Field& field : fields) {
        if (field._byHeader) {
            const llvm::StringRef header {field._header.data(), field._header.size()};
            fieldAttrs.push_back(_opBuilder.getStringAttr(header));
        } else {
            fieldAttrs.push_back(positionAttr(field._index));
        }

        fieldDecls.push_back(field._decl);
    }

    // A query naming no field still runs over the file's records - one CREATE per record,
    // a tally of them - and a row set needs a column to ride: the first field stands for
    // them, since every record carries one. No access names it, so it is published under
    // no declaration - only the rows it holds are read.
    if (fieldAttrs.empty()) {
        fieldAttrs.push_back(positionAttr(0));
        fieldDecls.push_back(nullptr);
    }

    const std::string_view path = loadCSVStmt->getFilePath().get();
    const mlir::StringAttr pathAttr = _opBuilder.getStringAttr(llvm::StringRef {path.data(), path.size()});
    const mlir::ArrayAttr fieldsAttr = _opBuilder.getArrayAttr(fieldAttrs);

    const mlir::db::ColumnType fieldType = allocColumnType(mlir::storage::OwnedStringType::get(_mlirCtxt));
    const llvm::SmallVector<mlir::Type> fieldTypes(fieldAttrs.size(), fieldType);
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    const auto emitLoad = [&]() -> mlir::db::LoadCSV {
        return _opBuilder.create<mlir::db::LoadCSV>(loc,
                                                    fieldTypes,
                                                    pathAttr,
                                                    fieldsAttr,
                                                    loadCSVStmt->hasHeaders(),
                                                    loadCSVStmt->skipOnError());
    };

    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    if (inFlight._columns.empty()) {
        publishLoadCSVFields(loadCSVStmt, fieldDecls, emitLoad().getResults());
        return;
    }

    // The load reads no column at all, so it produces the same records for every row
    // already in flight: their cartesian product, which is what a search reading none of
    // them is paired with too.
    mlir::Block* const currentBlock = _opBuilder.getInsertionBlock();

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    resultTypes.append(fieldTypes.begin(), fieldTypes.end());

    _opBuilder.setInsertionPointToEnd(currentBlock);
    mlir::db::CrossProduct crossProduct = _opBuilder.create<mlir::db::CrossProduct>(loc, resultTypes);

    mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();
    mlir::Block* const rightBlock = &crossProduct.getRightFactor().front();

    moveDataflowIntoLeftFactor(crossProduct);
    hoistConstantsOutOfLeftFactor(crossProduct);

    _opBuilder.setInsertionPointToEnd(leftBlock);
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {inFlight._columns});

    _opBuilder.setInsertionPointToEnd(rightBlock);
    mlir::db::LoadCSV load = emitLoad();
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {load.getResults()});

    // Anything generated from here on reads the product's results, not the factors'.
    _opBuilder.setInsertionPointToEnd(currentBlock);

    const mlir::Operation::result_range results = crossProduct.getResults();
    rebindInFlightColumns(results, /*firstResult=*/0, inFlight);

    publishLoadCSVFields(loadCSVStmt, fieldDecls, results.drop_front(inFlight._columns.size()));
}

void DBProgramGenerator::publishLoadCSVFields(const LoadCSVStmt* loadCSVStmt,
                                              llvm::ArrayRef<const VarDecl*> fieldDecls,
                                              mlir::ResultRange fields) {
    bioassert(fieldDecls.size() == fields.size(), "One declaration per loaded field expected");

    // The row a load bound has no column of its own, so the field columns are named after
    // it: what the projection prints for an unaliased row[0] is the item's own text, so
    // these names are only what a standalone load and a WITH read them under.
    const std::string_view alias = loadCSVStmt->getAliasDecl()->getName();

    for (size_t index = 0; index < fieldDecls.size(); index++) {
        _part._yieldedColumns.push_back({fieldDecls[index], alias, fields[index]});
    }
}

void DBProgramGenerator::generateVectorSearch(const VectorSearchStmt* vectorSearchStmt) {
    const EmbeddingLiteral* queryVector = vectorSearchStmt->getQueryVector();
    if (!queryVector) {
        throw TuringException("VECTOR SEARCH statement has no query vector.");
    }

    const YieldClause* yield = vectorSearchStmt->getYield();
    const YieldItems* yieldItems = yield ? yield->getItems() : nullptr;
    if (!yieldItems || yieldItems->getItems().empty()) {
        throw TuringException("VECTOR SEARCH statement names no yielded value.");
    }

    const std::span<const float> queryValues = queryVector->getValue();
    const mlir::DenseF32ArrayAttr queryVectorAttr =
        _opBuilder.getDenseF32ArrayAttr(llvm::ArrayRef<float> {queryValues.data(), queryValues.size()});

    const std::string_view searchedIndex = vectorSearchStmt->getIndexName();
    const llvm::StringRef indexName {searchedIndex.data(), searchedIndex.size()};
    const uint64_t neighbourCount = vectorSearchStmt->getK();

    const mlir::db::ColumnType idType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType scoreType = allocColumnType(_opBuilder.getF64Type());
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    if (inFlight._columns.empty()) {
        mlir::db::VectorSearch search = _opBuilder.create<mlir::db::VectorSearch>(loc,
                                                                                  idType,
                                                                                  scoreType,
                                                                                  indexName,
                                                                                  neighbourCount,
                                                                                  queryVectorAttr);

        publishVectorSearchYields(yieldItems, search.getIds(), search.getScores());
        generateYieldFilter(yieldItems);
        return;
    }

    // The search takes no column at all, so it produces the same neighbours for every row
    // already in flight: their cartesian product, which is what a call reading none of
    // them is paired with too.
    mlir::Block* const currentBlock = _opBuilder.getInsertionBlock();

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    resultTypes.push_back(idType);
    resultTypes.push_back(scoreType);

    _opBuilder.setInsertionPointToEnd(currentBlock);
    mlir::db::CrossProduct crossProduct = _opBuilder.create<mlir::db::CrossProduct>(loc, resultTypes);

    mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();
    mlir::Block* const rightBlock = &crossProduct.getRightFactor().front();

    moveDataflowIntoLeftFactor(crossProduct);
    hoistConstantsOutOfLeftFactor(crossProduct);

    _opBuilder.setInsertionPointToEnd(leftBlock);
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {inFlight._columns});

    _opBuilder.setInsertionPointToEnd(rightBlock);
    mlir::db::VectorSearch search = _opBuilder.create<mlir::db::VectorSearch>(loc,
                                                                              idType,
                                                                              scoreType,
                                                                              indexName,
                                                                              neighbourCount,
                                                                              queryVectorAttr);
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {search.getIds(), search.getScores()});

    // Anything generated from here on reads the product's results, not the factors'.
    _opBuilder.setInsertionPointToEnd(currentBlock);

    const mlir::Operation::result_range results = crossProduct.getResults();
    rebindInFlightColumns(results, /*firstResult=*/0, inFlight);

    const size_t searchOffset = inFlight._columns.size();
    publishVectorSearchYields(yieldItems, results[searchOffset], results[searchOffset + 1]);

    generateYieldFilter(yieldItems);
}

void DBProgramGenerator::publishVectorSearchYields(const YieldItems* yieldItems,
                                                   mlir::Value ids,
                                                   mlir::Value scores) {
    for (const SymbolExpr* item : *yieldItems) {
        const Symbol* symbol = item->getSymbol();

        // The statement's own name for the value - the one an alias renamed - is what picks
        // the column, and the analyzer has already rejected every name but these two.
        const bool isScore = symbol->getOriginalName() == vectorSearchScoreYield;

        _part._yieldedColumns.push_back({item->getDecl(),
                                         symbol->getName(),
                                         isScore ? scores : ids,
                                         /*isResult=*/true});
    }
}

void DBProgramGenerator::generateUnwind(const UnwindStmt* unwind) {
    const Symbol* symbol = unwind->symbol();
    bioassert(symbol, "UNWIND without a variable.");

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    const mlir::Value source = getOrTranslateExprColumn(variableColumns, unwind->arg());

    // Everything already in flight rides through the carry set, replicated once per row
    // the cell beside it unwound into, so the rest of the query still reads it row-aligned
    // with the elements.
    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    // The element column's value type is the source's to decide and is resolved during
    // lowering, as a property fetch's value column is. A carried column keeps its own
    // type: the unwind replicates its rows, it never retypes them.
    llvm::SmallVector<mlir::Type> resultTypes {mlir::db::ColumnType::get(_mlirCtxt)};
    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    auto unwindOp = _opBuilder.create<mlir::db::Unwind>(_opBuilder.getUnknownLoc(),
                                                        resultTypes,
                                                        source,
                                                        mlir::ValueRange {inFlight._columns});

    const mlir::Operation::result_range results = unwindOp.getResults();
    rebindInFlightColumns(results, /*firstResult=*/1, inFlight);

    _part._yieldedColumns.push_back({unwind->getDecl(), symbol->getName(), results[0], /*isResult=*/false});
}

void DBProgramGenerator::generateYieldFilter(const YieldItems* yieldItems) {
    const WhereClause* where = yieldItems ? yieldItems->getWhereClause() : nullptr;
    if (!where) {
        return;
    }

    // The predicate reads what the call produced, so it filters the rows the call emitted
    // rather than the ones it was given - the MATCH's WHERE, applied one stage later. Every
    // column in flight goes through the filter, the yields among them, so they stay
    // row-aligned with each other.
    const Expr* predicateExpr = where->getExpr();
    translateExpr(predicateExpr);

    const auto findIt = _part._exprMap.find(predicateExpr);
    bioassert(findIt != end(_part._exprMap), "Failed to get value for YIELD WHERE expr");

    filterAllColumns(findIt->second);
}

void DBProgramGenerator::moveDataflowIntoLeftFactor(mlir::db::CrossProduct crossProduct) {
    mlir::Block* const currentBlock = crossProduct->getBlock();
    mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();

    mlir::Block::OpListType& blockOps = currentBlock->getOperations();
    leftBlock->getOperations().splice(leftBlock->end(),
                                     blockOps,
                                     blockOps.begin(),
                                     crossProduct->getIterator());
}

void DBProgramGenerator::hoistConstantsOutOfLeftFactor(mlir::db::CrossProduct crossProduct) {
    mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();

    // Every operand of a constant column is itself a constant column, so the whole cone
    // moves and nothing left outside reads what stayed in. Each op of that cone is asked
    // in turn, so an answer is kept for the ops above it rather than rewalked under each.
    llvm::DenseMap<mlir::Value, bool> classified;

    llvm::SmallVector<mlir::Operation*> constantOps;
    for (mlir::Operation& op : *leftBlock) {
        const bool yieldsAConstant = op.getNumResults() > 0 && yieldsConstantColumn(op.getResult(0), classified);

        if (yieldsAConstant) {
            constantOps.push_back(&op);
        }
    }

    for (mlir::Operation* op : constantOps) {
        op->moveBefore(crossProduct);
    }
}

void DBProgramGenerator::generateCrossedCall(std::string_view procedureName,
                                             llvm::ArrayRef<mlir::Attribute> yieldedNames,
                                             llvm::SmallVectorImpl<YieldedColumn>& yielded,
                                             mlir::ValueRange inputs,
                                             const InFlightColumns& inFlight) {
    // The call reads none of the rows already in flight, so it produces the same rows for
    // every one of them: their cartesian product. Each side becomes a factor of a
    // db.cross_product - what the query has matched so far on the left, the call on the
    // right - and the product pairs them, which is exactly what it exists for.
    mlir::Block* const currentBlock = _opBuilder.getInsertionBlock();

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    for (size_t yieldIndex = 0; yieldIndex < yieldedNames.size(); yieldIndex++) {
        resultTypes.push_back(mlir::db::ColumnType::get(_mlirCtxt));
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    _opBuilder.setInsertionPointToEnd(currentBlock);
    auto crossProduct = _opBuilder.create<mlir::db::CrossProduct>(loc, resultTypes);

    mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();
    mlir::Block* const rightBlock = &crossProduct.getRightFactor().front();

    moveDataflowIntoLeftFactor(crossProduct);

    // The argument columns were built into the block that has just become the left factor,
    // but the call reading them is the right one and a value cannot cross a region
    // boundary. None of them reads what the left factor yields - that is what makes this a
    // product - so the whole chain behind them moves across intact, in its original order.
    llvm::DenseSet<mlir::Operation*> argumentOps;
    collectDefiningOps(inputs, argumentOps);

    llvm::SmallVector<mlir::Operation*> argumentOpsInOrder;
    for (mlir::Operation& op : *leftBlock) {
        if (argumentOps.contains(&op)) {
            argumentOpsInOrder.push_back(&op);
        }
    }

    for (mlir::Operation* op : argumentOpsInOrder) {
        op->moveBefore(rightBlock, rightBlock->end());
    }

    hoistConstantsOutOfLeftFactor(crossProduct);

    _opBuilder.setInsertionPointToEnd(leftBlock);
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {inFlight._columns});

    // The call is the right factor, opening its own dataflow there: it carries nothing,
    // since the rows it is paired with are the other factor's.
    llvm::SmallVector<mlir::Type> callResultTypes;
    for (size_t yieldIndex = 0; yieldIndex < yieldedNames.size(); yieldIndex++) {
        callResultTypes.push_back(mlir::db::ColumnType::get(_mlirCtxt));
    }

    _opBuilder.setInsertionPointToEnd(rightBlock);
    auto callOp = _opBuilder.create<mlir::db::CallProcedure>(loc,
                                                            callResultTypes,
                                                            _opBuilder.getStringAttr(procedureName),
                                                            inputs,
                                                            mlir::ValueRange {},
                                                            _opBuilder.getArrayAttr(yieldedNames));
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {callOp.getResults()});

    // Anything generated from here on reads the product's results, not the factors'.
    _opBuilder.setInsertionPointToEnd(currentBlock);

    // Its results are the left factor's columns then the right's: the rows already in
    // flight, now paired, followed by the call's own.
    const mlir::Operation::result_range results = crossProduct.getResults();
    rebindInFlightColumns(results, /*firstResult=*/0, inFlight);

    for (size_t yieldIndex = 0; yieldIndex < yielded.size(); yieldIndex++) {
        yielded[yieldIndex]._column = results[inFlight._columns.size() + yieldIndex];
        _part._yieldedColumns.push_back(yielded[yieldIndex]);
    }
}

void DBProgramGenerator::publishCreatedEntity(const VarDecl* decl,
                                              mlir::Value column,
                                              llvm::ArrayRef<llvm::StringRef> propNames,
                                              llvm::ArrayRef<mlir::Value> propValues) {
    bioassert(propNames.size() == propValues.size(), "One value per created property expected");

    PartScope::CreatedEntity& created = _part._createdEntities[decl];
    created._column = column;

    for (size_t index = 0; index < propNames.size(); index++) {
        const llvm::StringRef propName = propNames[index];
        created._properties[std::string_view {propName.data(), propName.size()}] = propValues[index];
    }
}

void DBProgramGenerator::generateCreate(const SinglePartQuery* query) {
    const StmtContainer* updateStmts = query->getUpdateStmts();
    if (!updateStmts) {
        return;
    }

    // Collect the columns a MATCH bound or a CALL yielded, by variable, so CREATE
    // patterns can reference them.
    std::unordered_map<const VarDecl*, mlir::Value> knownVars;
    for (const auto& [var, identities] : _part._varMap) {
        const VarDecl* decl = var->getDecl();
        if (decl && !identities.empty()) {
            knownVars[decl] = identities.back();
        }
    }

    for (const YieldedColumn& yieldedColumn : _part._yieldedColumns) {
        if (yieldedColumn._decl && isRowAlignedHere(yieldedColumn._column)) {
            knownVars[yieldedColumn._decl] = yieldedColumn._column;
        }
    }

    // The pattern's own order picks the column, since reading an arbitrary bucket of
    // knownVars would make the number of nodes written depend on where a name hashed. A
    // scope of nothing but yielded columns has no such order to read, so it falls back on
    // one of them
    mlir::Value matchCardinality = resolveWildcardColumn();
    if (!matchCardinality && !knownVars.empty()) {
        matchCardinality = knownVars.begin()->second;
    }

    const mlir::db::ColumnType nodeIDType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType edgeIDType = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    // Emit db.create_node for a node that is not already in knownVars, then register
    // the result under the node's variable so later patterns can reference it.
    const auto resolveOrCreateNode = [&](const NodePattern* node) -> mlir::Value {
        const VarDecl* decl = node->getDecl();

        if (decl && knownVars.contains(decl)) {
            return knownVars.at(decl);
        }

        llvm::SmallVector<llvm::StringRef> labelNames;
        const SymbolChain* labels = node->labels();
        if (labels) {
            for (const Symbol* sym : *labels) {
                const std::string_view symName = sym->getName();
                labelNames.push_back(llvm::StringRef(symName.data(), symName.size()));
            }
        }

        llvm::SmallVector<llvm::StringRef> propNames;
        llvm::SmallVector<mlir::Value> propValues;
        const NodePatternData* data = node->getData();
        if (data) {
            for (const EntityPropertyConstraint& constraint : data->exprConstraints()) {
                translateExpr(constraint._expr);
                const std::string_view propName = constraint._propTypeName;
                propNames.push_back(llvm::StringRef(propName.data(), propName.size()));
                propValues.push_back(_part._exprMap.at(constraint._expr));
            }
        }

        const mlir::Value cardinality = matchCardinality;

        mlir::db::CreateNode createNode = _opBuilder.create<mlir::db::CreateNode>(
            loc,
            nodeIDType,
            _opBuilder.getStrArrayAttr(labelNames),
            _opBuilder.getStrArrayAttr(propNames),
            mlir::ValueRange{propValues},
            cardinality);
        const mlir::Value nodeValue = createNode.getResult();

        if (decl) {
            knownVars[decl] = nodeValue;
            publishCreatedEntity(decl, nodeValue, propNames, propValues);
        }

        return nodeValue;
    };

    llvm::SmallVector<llvm::StringRef> propNames;
    llvm::SmallVector<mlir::Value> propValues;

    for (const Stmt* stmt : updateStmts->stmts()) {
        if (stmt->getKind() != Stmt::Kind::CREATE) {
            continue;
        }
        const CreateStmt* createStmt = static_cast<const CreateStmt*>(stmt);
        const Pattern* pattern = createStmt->getPattern();

        for (const PatternElement* element : pattern->elements()) {
            const EntityPattern* entPtn = element->getRootEntity();
            const auto* nodePtn = dynamic_cast<const NodePattern*>(entPtn);
            bioassert(nodePtn, "Unknown root entity");

            mlir::Value lhsValue = resolveOrCreateNode(nodePtn);

            for (auto [edge, targetNode] : element->getElementChain()) {
                const mlir::Value rhsValue = resolveOrCreateNode(targetNode);

                propNames.clear();
                propValues.clear();
                const EdgePatternData* edgeData = edge->getData();
                if (edgeData) {
                    for (const EntityPropertyConstraint& constraint : edgeData->exprConstraints()) {
                        translateExpr(constraint._expr);
                        const std::string_view propName = constraint._propTypeName;
                        propNames.push_back(llvm::StringRef(propName.data(), propName.size()));
                        propValues.push_back(_part._exprMap.at(constraint._expr));
                    }
                }

                bioassert(edgeData && !edgeData->edgeTypeConstraints().empty(),
                          "CREATE edge must have an edge type");
                const std::string_view edgeType = edgeData->edgeTypeConstraints().front();

                const mlir::Value srcValue =
                    edge->getDirection() == EdgePattern::Direction::Backward ? rhsValue : lhsValue;
                const mlir::Value tgtValue =
                    edge->getDirection() == EdgePattern::Direction::Backward ? lhsValue : rhsValue;

                mlir::db::CreateEdge createEdge = _opBuilder.create<mlir::db::CreateEdge>(
                    loc,
                    edgeIDType,
                    srcValue,
                    tgtValue,
                    _opBuilder.getStringAttr(edgeType),
                    _opBuilder.getStrArrayAttr(propNames),
                    mlir::ValueRange{propValues});

                const VarDecl* edgeDecl = edge->getDecl();
                if (edgeDecl && !edgeDecl->isUnnamed()) {
                    publishCreatedEntity(edgeDecl, createEdge.getResult(), propNames, propValues);
                }

                lhsValue = rhsValue;
            }
        }
    }
}

mlir::Value DBProgramGenerator::resolveEntityColumn(const VarDecl* decl) {
    for (const auto& [var, values] : _part._varMap) {
        if (var->getDecl() == decl && !values.empty()) {
            return values.back();
        }
    }

    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();
    const auto findIt = edgeIdentities.find(decl);
    const bool foundEdgeIdentity = findIt != edgeIdentities.end() && !findIt->second.empty();
    if (foundEdgeIdentity) {
        const VariableDependency* representative = findIt->second.front();
        return _part._varMap.at(representative).back();
    }

    const auto createdIt = _part._createdEntities.find(decl);
    if (createdIt != end(_part._createdEntities)) {
        return createdIt->second._column;
    }

    return findYieldedColumn(decl);
}

mlir::Value DBProgramGenerator::resolveColumnInScope(ColumnPredicate accept) const {
    for (const VariableDependency& var : _vdg.vars()) {
        const auto findIt = _part._varMap.find(&var);
        if (findIt == _part._varMap.end() || findIt->second.empty()) {
            continue;
        }

        const mlir::Value column = findIt->second.back();
        if (!isRowAlignedHere(column) || !accept(column)) {
            continue;
        }

        return column;
    }

    for (const YieldedColumn& yielded : _part._yieldedColumns) {
        if (isRowAlignedHere(yielded._column)) {
            return yielded._column;
        }
    }

    return mlir::Value {};
}

mlir::Value DBProgramGenerator::translateAggregateInput(const Expr* argExpr,
                                                        const VariableColumnMap* variableColumns) {
    const EvaluatedType argType = argExpr->getType();
    const bool isEntity = argType == EvaluatedType::NodePattern || argType == EvaluatedType::EdgePattern;

    if (isEntity) {
        if (variableColumns) {
            return getOrTranslateExprColumn(*variableColumns, argExpr);
        }

        return getOrTranslateExprColumn(argExpr);
    } else if (argType != EvaluatedType::Wildcard) {
        translateExpr(argExpr);
        return _part._exprMap.at(argExpr);
    }

    const mlir::Value column = resolveWildcardColumn();
    if (column) {
        return column;
    }

    // A query binding no column runs over one row of its own, and count(*) tallies
    // that row: a constant stands for it, as the 42 of RETURN count(42) does
    const bool bindsNoColumn = _part._varMap.empty() && _part._yieldedColumns.empty();
    bioassert(bindsNoColumn, "count(*) over no column holding the rows it counts.");

    const mlir::TypedAttr oneAttr = _opBuilder.getI64IntegerAttr(1);
    const mlir::db::ColumnType oneType = allocColumnType(oneAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), oneType, oneAttr).getResult();
}

mlir::Value DBProgramGenerator::resolveRowCarryingColumn() const {
    return resolveColumnInScope([](mlir::Value column) { return !yieldsConstantColumn(column); });
}

mlir::Value DBProgramGenerator::resolveWildcardColumn() const {
    const mlir::Value rowColumn = resolveRowCarryingColumn();
    if (rowColumn) {
        return rowColumn;
    }

    return resolveColumnInScope([](mlir::Value) { return true; });
}

void DBProgramGenerator::generateSet(const SinglePartQuery* query) {
    const StmtContainer* updateStmts = query->getUpdateStmts();
    if (!updateStmts) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (const Stmt* stmt : updateStmts->stmts()) {
        if (stmt->getKind() != Stmt::Kind::SET) {
            continue;
        }

        const SetStmt* setStmt = static_cast<const SetStmt*>(stmt);

        for (const SetItem* item : setStmt->getItems()) {
            const SetItem::PropertyExprAssign* assign =
                std::get_if<SetItem::PropertyExprAssign>(&item->item());
            bioassert(assign, "Only property-assignment SET items are supported");

            const PropertyExpr* propertyExpr = assign->_propTypeExpr;
            const VarDecl* entityDecl = propertyExpr->getEntityVarDecl();
            const std::string_view varName = entityDecl->getName();
            const std::string_view propName = propertyExpr->getPropName();

            const mlir::Value entityColumn = resolveEntityColumn(entityDecl);
            bioassert(entityColumn, "SET on unknown variable: {}", varName);

            translateExpr(assign->_propValueExpr);
            const mlir::Value valueColumn = _part._exprMap.at(assign->_propValueExpr);

            const mlir::StringAttr propAttr = _opBuilder.getStringAttr(propName);
            const EvaluatedType entityType = entityDecl->getType();
            const bool isNode = entityType == EvaluatedType::NodePattern;
            const bool isEdge = entityType == EvaluatedType::EdgePattern;
            bioassert(isNode || isEdge, "SET on non-entity variable: {}", varName);

            if (isNode) {
                _opBuilder.create<mlir::db::SetNodeProperty>(loc, entityColumn, propAttr, valueColumn);
            } else /* (isEdge) */ {
                _opBuilder.create<mlir::db::SetEdgeProperty>(loc, entityColumn, propAttr, valueColumn);
            }
        }
    }
}

void DBProgramGenerator::generateDelete(const SinglePartQuery* query) {
    const StmtContainer* updateStmts = query->getUpdateStmts();
    if (!updateStmts) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (const Stmt* stmt : updateStmts->stmts()) {
        if (stmt->getKind() != Stmt::Kind::DELETE) {
            continue;
        }

        const DeleteStmt* deleteStmt = static_cast<const DeleteStmt*>(stmt);
        const bool detach = deleteStmt->isDetaching();

        for (const Expr* expr : *deleteStmt->getExpressions()) {
            if (expr->getKind() != Expr::Kind::SYMBOL) {
                throw TuringException("Expressions in DELETE statements can only be symbols");
            }

            const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(expr);
            const VarDecl* decl = symbolExpr->getDecl();
            bioassert(decl, "DELETE target symbol has no declaration");
            const std::string_view varName = decl->getName();

            const mlir::Value entityColumn = resolveEntityColumn(decl);
            if (!entityColumn) {
                throw TuringException("Cannot delete unbound variable: " + std::string(varName));
            }

            const EvaluatedType entityType = decl->getType();
            const bool isNode = entityType == EvaluatedType::NodePattern;
            const bool isEdge = entityType == EvaluatedType::EdgePattern;

            if (isNode) {
                _opBuilder.create<mlir::db::DeleteNode>(loc, entityColumn, detach);
            } else if (isEdge) {
                _opBuilder.create<mlir::db::DeleteEdge>(loc, entityColumn);
            } else {
                throw TuringException("Can only delete nodes or edges");
            }
        }
    }
}

void DBProgramGenerator::generateOutput(const Projection* projection) {
    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    llvm::SmallVector<mlir::Value> outputted;
    llvm::SmallVector<llvm::StringRef> outputNames;
    translateProjection(projection, variableColumns, outputted, outputNames);

    translateProjectionTail(projection, variableColumns, outputted);

    _opBuilder.create<mlir::db::Output>(_opBuilder.getUnknownLoc(),
                                       mlir::ValueRange {outputted},
                                       _opBuilder.getStrArrayAttr(outputNames));
}

// A standalone CALL ends no projection, so what it yielded is the result: the columns go
// out in yield order, under the names the YIELD gave them. A query that writes is not
// standalone whatever it yielded - its result is its RETURN, and it has none - so a CALL
// or a LOAD CSV feeding a CREATE reports no row rather than every row it wrote one for.
void DBProgramGenerator::generateYieldedOutput(const SinglePartQuery* query) {
    const StmtContainer* updateStmts = query->getUpdateStmts();
    if (updateStmts && !updateStmts->stmts().empty()) {
        return;
    }

    llvm::SmallVector<mlir::Value> yielded;
    llvm::SmallVector<llvm::StringRef> yieldedNames;
    for (const YieldedColumn& yieldedColumn : _part._yieldedColumns) {
        // An UNWIND's elements ride here so the rest of the part reads them, but they are
        // no result of their own: a query with no projection returns nothing.
        if (!yieldedColumn._isResult) {
            continue;
        }

        yielded.push_back(yieldedColumn._column);
        yieldedNames.push_back(llvm::StringRef(yieldedColumn._name.data(), yieldedColumn._name.size()));
    }

    if (yielded.empty()) {
        return;
    }

    _opBuilder.create<mlir::db::Output>(_opBuilder.getUnknownLoc(),
                                       mlir::ValueRange {yielded},
                                       _opBuilder.getStrArrayAttr(yieldedNames));
}

void DBProgramGenerator::generateWith(const WithStmt* with) {
    const Projection* projection = with->getProjection();

    generateGroupAggregate(projection);

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    llvm::SmallVector<mlir::Value> projected;
    llvm::SmallVector<llvm::StringRef> names;
    translateProjection(projection, variableColumns, projected, names);

    broadcastConstantProjection(projected);

    translateProjectionTail(projection, variableColumns, projected);

    publishBoundColumns(projection, names, projected);

    const WhereClause* where = with->getWhere();
    if (!where) {
        return;
    }

    std::vector<const Expr*> conjuncts;
    flattenConjuncts(where->getExpr(), conjuncts);

    applyPredicateFilters(conjuncts);
}

void DBProgramGenerator::broadcastConstantProjection(llvm::SmallVectorImpl<mlir::Value>& projected) {
    const bool constantsAlone = std::ranges::all_of(projected, [](mlir::Value column) {
        return yieldsConstantColumn(column);
    });

    if (!constantsAlone) {
        return;
    }

    const mlir::Value driver = resolveRowCarryingColumn();
    if (!driver) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    for (mlir::Value& column : projected) {
        column = _opBuilder.create<mlir::db::BroadcastConstant>(loc, noneType, column, driver).getResult();
    }
}

void DBProgramGenerator::publishBoundColumns(const Projection* projection,
                                             llvm::ArrayRef<llvm::StringRef> names,
                                             llvm::ArrayRef<mlir::Value> columns) {
    bioassert(names.size() == columns.size(), "One name per column a WITH publishes expected");
    bioassert(!names.empty(), "A WITH publishes at least one column");

    // The barrier opens a scope of its own, so what follows it reads these columns through
    // the declarations that scope holds and not through the ones the projected items carry
    const Projection::PublishedDecls& publishedDecls = projection->publishedDecls();
    bioassert(publishedDecls.size() == names.size(), "One declaration per column a WITH publishes expected");

    _part = PartScope {};
    _vdg.clear();

    for (size_t index = 0; index < names.size(); index++) {
        const llvm::StringRef name = names[index];
        bioassert(!name.empty(), "Column a WITH publishes without a name");

        const std::string_view boundName {name.data(), name.size()};
        registerValue(_vdg.registerBoundVariable(boundName, publishedDecls[index]), columns[index]);
    }
}

void DBProgramGenerator::publishInFlightColumns() {
    // A cut opens no scope of its own, so the part below reads these columns through the
    // very declarations the part above bound them to. Each name is copied out: the
    // variables holding them are the ones this clears, and the part below is opened with
    // them
    llvm::SmallVector<PublishedColumn> published;

    forEachVariableColumn([&published](const VarDecl* decl, std::string_view name, mlir::Value column) {
        const auto sameName = [name](const PublishedColumn& candidate) {
            return candidate._name == name;
        };

        const auto foundIt = std::ranges::find_if(published, sameName);
        if (foundIt != published.end()) {
            foundIt->_decl = decl;
            foundIt->_column = column;
            return;
        }

        published.push_back({decl, std::string(name), column});
    });

    // Under a name order rather than the map's, so the same query generates the same IR
    std::ranges::sort(published, [](const PublishedColumn& left, const PublishedColumn& right) {
        return left._name < right._name;
    });

    _part = PartScope {};
    _vdg.clear();

    for (const PublishedColumn& column : published) {
        registerValue(_vdg.registerBoundVariable(column._name, column._decl), column._column);
    }
}

void DBProgramGenerator::forEachVariableColumn(const VariableColumnBinding& bind) const {
    for (const auto& [cypherVar, mlirCol] : _part._varMap) {
        const std::string_view varName = cypherVar->getName();

        bioassert(not mlirCol.empty(), "No definitions for {}", varName);

        bind(cypherVar->getDecl(), varName, mlirCol.back());
    }

    // A CALL's yielded columns are variables of the query too, declared by the YIELD.
    for (const YieldedColumn& yieldedColumn : _part._yieldedColumns) {
        bind(yieldedColumn._decl, yieldedColumn._name, yieldedColumn._column);
    }

    // So are the entities a CREATE wrote, declared by its pattern
    for (const auto& [createdDecl, created] : _part._createdEntities) {
        bind(createdDecl, createdDecl->getName(), created._column);
    }

    for (const auto& [decl, vars] : _vdg.edgeIdentities()) {
        bioassert(!vars.empty(), "Empty edge identity for '{}'", decl->getName());
        const VariableDependency* representative = vars.front();
        bioassert(_part._varMap.contains(representative), "Edge identity representative not in varMap");
        bind(decl, decl->getName(), _part._varMap.at(representative).back());
    }
}

void DBProgramGenerator::collectVariableColumns(VariableColumnMap& variableColumns) const {
    forEachVariableColumn([&variableColumns](const VarDecl* decl, std::string_view name, mlir::Value column) {
        if (decl) {
            variableColumns[decl] = column;
        }
    });
}

mlir::Value DBProgramGenerator::findVariableColumn(const VarDecl* decl) const {
    if (!decl) {
        return mlir::Value {};
    }

    mlir::Value found;
    forEachVariableColumn([&found, decl](const VarDecl* boundDecl, std::string_view name, mlir::Value column) {
        if (boundDecl == decl) {
            found = column;
        }
    });

    return found;
}

void DBProgramGenerator::translateProjection(const Projection* projection,
                                             const VariableColumnMap& variableColumns,
                                             llvm::SmallVectorImpl<mlir::Value>& projected,
                                             llvm::SmallVectorImpl<llvm::StringRef>& names) {
    const auto getVarForItem = [&](auto&& item) -> mlir::Value {
        using Type = std::remove_cvref_t<decltype(item)>;

        if constexpr (std::is_same_v<Type, VarDecl*>) {
            const auto findIt = variableColumns.find(item);
            bioassert(findIt != end(variableColumns), "Return variable '{}' not found", item->getName());
            return findIt->second;
        } else {
            return getOrTranslateExprColumn(variableColumns, item);
        }
    };

    // The analyzer names every item it accepts, so an item without one is left for the
    // sink to label by position rather than being a codegen error.
    const auto getNameForItem = [&](auto&& item) -> llvm::StringRef {
        const std::optional<std::string_view> name = projection->getName(item);
        if (!name) {
            return llvm::StringRef();
        }

        return llvm::StringRef(name->data(), name->size());
    };

    for (const Projection::ReturnItem item : projection->items()) {
        const mlir::Value itemCol = std::visit(getVarForItem, item);
        projected.push_back(itemCol);
        names.push_back(std::visit(getNameForItem, item));

        // An alias is one variable declared once, so a key naming it holds the very
        // declaration of the item it names: publishing the column under that declaration
        // is what lets the key read this column instead of computing a second one
        Expr* const* itemExpr = std::get_if<Expr*>(&item);
        if (!itemExpr) {
            continue;
        }

        const VarDecl* itemDecl = (*itemExpr)->getExprVarDecl();
        if (itemDecl) {
            _part._projectedColumns[itemDecl] = itemCol;
        }
    }
}

void DBProgramGenerator::translateProjectionTail(const Projection* projection,
                                                 const VariableColumnMap& variableColumns,
                                                 llvm::SmallVectorImpl<mlir::Value>& projected) {
    // DISTINCT dedups the projection, and everything after it works on the rows that
    // survive: the sort orders the distinct rows, and SKIP and LIMIT cut them
    if (projection->isDistinct()) {
        translateDistinct(projection, projected);
    }

    // ORDER BY reorders the whole projection, so it comes before them too: SKIP and
    // LIMIT cut the sorted rows
    if (projection->hasOrderBy()) {
        translateOrderBy(projection, variableColumns, projected);
    }

    if (projection->hasSkip()) {
        translateCut<mlir::db::Skip>(projection, projection->getSkip()->getExpr(), "SKIP", projected);
    }

    if (projection->hasLimit()) {
        translateCut<mlir::db::Limit>(projection, projection->getLimit()->getExpr(), "LIMIT", projected);
    }
}

template <typename CutOp>
void DBProgramGenerator::translateCut(const Projection* projection,
                                      const Expr* countExpr,
                                      std::string_view clauseName,
                                      llvm::SmallVectorImpl<mlir::Value>& projected) {
    const int64_t countValue = evaluateConstantInteger(countExpr);

    if (countValue < 0) {
        throw TuringException(fmt::format("{} expression must be a non-negative integer", clauseName));
    }

    llvm::SmallVector<size_t> cutItems;
    llvm::SmallVector<mlir::Value> cut;
    collectCutColumns(projection, projected, cutItems, cut);

    llvm::SmallVector<mlir::Type> cutResultTypes;
    for (const mlir::Value column : cut) {
        cutResultTypes.push_back(column.getType());
    }

    auto cutOp = _opBuilder.create<CutOp>(_opBuilder.getUnknownLoc(),
                                          cutResultTypes,
                                          mlir::ValueRange {cut},
                                          static_cast<uint64_t>(countValue));

    const mlir::ResultRange cutResults = cutOp.getResults();
    for (size_t resultIndex = 0; resultIndex < cutItems.size(); resultIndex++) {
        projected[cutItems[resultIndex]] = cutResults[resultIndex];
    }
}

void DBProgramGenerator::translateDistinct(const Projection* projection,
                                           llvm::SmallVectorImpl<mlir::Value>& projected) {
    // An aggregate projection emits one row per group, keyed by the grouping keys, so no
    // two of its rows can be equal and the dedup would drop nothing. It could not read a
    // count either: that column is neither an ID nor a nullable value.
    if (projection->isAggregate()) {
        return;
    }

    // A constant column holds the same value in every row, so it tells no two rows apart:
    // the dedup reads the columns that vary and a constant one rides along untouched, as
    // ORDER BY drops a constant key
    llvm::SmallVector<size_t> dedupedItems;
    llvm::SmallVector<mlir::Value> dedupedColumns;
    collectRowColumns(projection, projected, dedupedItems, dedupedColumns);

    // Every column is constant, so the projection is one row repeated as many times as
    // the query matched: no column tells two of those rows apart, and the one the dedup
    // keeps is the first of them - the projection capped at a single row
    if (dedupedColumns.empty()) {
        translateDistinctOverConstants(projection, projected);
        return;
    }

    llvm::SmallVector<mlir::Type> dedupedTypes;
    for (const mlir::Value column : dedupedColumns) {
        dedupedTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto distinctOp = _opBuilder.create<mlir::db::RemoveDuplicates>(loc, dedupedTypes, mlir::ValueRange{dedupedColumns});

    // The dedup hands back one column per column it read, so its results take the place
    // of the ones it was given and the constant columns stay as they were
    const mlir::ResultRange results = distinctOp.getResults();
    for (size_t resultIndex = 0; resultIndex < dedupedItems.size(); resultIndex++) {
        projected[dedupedItems[resultIndex]] = results[resultIndex];
    }
}

void DBProgramGenerator::translateDistinctOverConstants(const Projection* projection,
                                                        llvm::SmallVectorImpl<mlir::Value>& projected) {
    llvm::SmallVector<size_t> cappedItems;
    llvm::SmallVector<mlir::Value> capped;
    collectCutColumns(projection, projected, cappedItems, capped);

    llvm::SmallVector<mlir::Type> cappedTypes;
    for (const mlir::Value column : capped) {
        cappedTypes.push_back(column.getType());
    }

    const uint64_t keptRowCount = 1;

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto limitOp = _opBuilder.create<mlir::db::Limit>(loc, cappedTypes, mlir::ValueRange{capped}, keptRowCount);

    const mlir::ResultRange results = limitOp.getResults();
    for (size_t resultIndex = 0; resultIndex < cappedItems.size(); resultIndex++) {
        projected[cappedItems[resultIndex]] = results[resultIndex];
    }
}

void DBProgramGenerator::translateOrderBy(const Projection* projection,
                                          const VariableColumnMap& variableColumns,
                                          llvm::SmallVectorImpl<mlir::Value>& projected) {
    const OrderBy* orderBy = projection->getOrderBy();
    const OrderBy::ItemVector& items = orderBy->getItems();
    bioassert(!items.empty(), "ORDER BY without a key");

    const size_t projectedCount = projection->items().size();

    // A sort reorders the rows of every column it is given at once, so it is given the
    // projected columns that carry rows and they stay row-aligned. A key the projection
    // does not carry - the a.age of RETURN a ORDER BY a.age - is handed over as one more
    // column, so that it moves with the row it belongs to; its result is then left
    // unread, which is what keeps it out of the output
    llvm::SmallVector<size_t> sortedItems;
    llvm::SmallVector<mlir::Value> sorted;
    collectRowColumns(projection, projected, sortedItems, sorted);

    llvm::SmallVector<int64_t> keyColumns;
    llvm::SmallVector<bool> keyAscending;

    // Keys are given most significant first, the order the Sort expects
    for (const OrderByItem* item : items) {
        const Expr* keyExpr = item->getExpr();

        const size_t projectedIndex = projection->findItemIndex(keyExpr);
        const bool isProjected = projectedIndex < projectedCount;
        const auto sortedItem = std::find(sortedItems.begin(), sortedItems.end(), projectedIndex);

        // A constant key holds the same value in every row, so it changes no order:
        // ORDER BY 1, n.name orders by n.name alone. An alias is only another spelling of
        // the item it was given to, so a key naming the alias of a constant item is that
        // same constant key - and the sort was handed no column to key it on either
        //
        // An aggregate is not constant however it spells its argument: count(*) reads no
        // row and still takes a value per group, which is what orders the groups
        const bool isConstantKey = !keyExpr->isDynamic() && !keyExpr->isAggregate();
        const bool namesAConstantItem = isProjected && sortedItem == sortedItems.end();

        if (isConstantKey || namesAConstantItem) {
            continue;
        }

        // A key names the column to sort by through its position in the columns handed to
        // the sort: the position the projected column was given there, or the end of the
        // set when the key has to be appended - so two appended keys never share a position
        if (isProjected) {
            keyColumns.push_back(static_cast<int64_t>(std::distance(sortedItems.begin(), sortedItem)));
        } else {
            const mlir::Value keyColumn = getOrTranslateExprColumn(variableColumns, keyExpr);

            // A key the projection does not carry is read into a column of its own, which
            // is constant when the key computes over constants alone: one value for every
            // row, so it orders nothing and there is no per-row column to key on
            if (yieldsConstantColumn(keyColumn)) {
                continue;
            }

            sorted.push_back(keyColumn);
            keyColumns.push_back(static_cast<int64_t>(sorted.size() - 1));
        }

        keyAscending.push_back(item->getType() == OrderByType::ASC);
    }

    // Every key was constant, so a sort would hand back the rows it was given in the
    // order it was given them: the projection is already what the ORDER BY asks for
    if (keyColumns.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Type> sortResultTypes;
    for (const mlir::Value column : sorted) {
        sortResultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto sortOp = _opBuilder.create<mlir::db::Sort>(loc,
                                                    sortResultTypes,
                                                    mlir::ValueRange {sorted},
                                                    keyColumns,
                                                    keyAscending);

    // The columns pass through the sort in place, so each column it was given comes back as
    // its own result; the extra key columns end the result range and are left unread, and a
    // constant column was never handed over
    const mlir::ResultRange results = sortOp.getResults();
    for (size_t resultIndex = 0; resultIndex < sortedItems.size(); resultIndex++) {
        projected[sortedItems[resultIndex]] = results[resultIndex];
    }
}

mlir::Value DBProgramGenerator::getOrTranslateExprColumn(const VariableColumnMap& variableColumns,
                                                         const Expr* expr) {
    // One column is held per variable, so an expression only has a column to be found
    // there when it is a variable and nothing else. Anything more is a computation over
    // columns, and has to be translated - as is a symbol naming no variable but the alias
    // of a projected item, which the translation resolves through the item that published
    // that column
    if (expr->getKind() == Expr::Kind::SYMBOL) {
        const VarDecl* var = expr->getExprVarDecl();
        bioassert(var, "Symbol expression without a declaration.");

        const auto findIt = variableColumns.find(var);
        if (findIt != end(variableColumns)) {
            return findIt->second;
        }
    }

    translateExpr(expr);
    return _part._exprMap.at(expr);
}

// The same, for a caller holding no map: the one name the expression could name is looked
// up over the bindings rather than every one of them being gathered to answer for it
mlir::Value DBProgramGenerator::getOrTranslateExprColumn(const Expr* expr) {
    if (expr->getKind() == Expr::Kind::SYMBOL) {
        const VarDecl* var = expr->getExprVarDecl();
        bioassert(var, "Symbol expression without a declaration.");

        if (const mlir::Value column = findVariableColumn(var)) {
            return column;
        }
    }

    translateExpr(expr);
    return _part._exprMap.at(expr);
}

void DBProgramGenerator::applyPredicateFilters(std::span<const Expr* const> predicates) {
    for (const Expr* predicate : predicates) {
        translateExpr(predicate);
        filterAllColumns(_part._exprMap.at(predicate));
    }
}

void DBProgramGenerator::generateMatchConstraints(const MatchStmt* matchStmt) {
    const Pattern* pattern = matchStmt->getPattern();

    std::vector<const Expr*> constraintExprs;

    for (const PatternElement* element : pattern->elements()) {
        const NodePattern* rootNode = static_cast<const NodePattern*>(element->getRootEntity());
        const NodePatternData* rootData = rootNode->getData();

        if (rootData) {
            for (const EntityPropertyConstraint& constraint : rootData->exprConstraints()) {
                constraintExprs.push_back(constraint._expr);
            }
        }

        for (auto [edgePattern, nodePattern] : element->getElementChain()) {
            const EdgePatternData* edgeData = edgePattern->getData();
            if (edgeData) {
                for (const EntityPropertyConstraint& constraint : edgeData->exprConstraints()) {
                    constraintExprs.push_back(constraint._expr);
                }
            }

            const NodePatternData* nodeData = nodePattern->getData();
            if (nodeData) {
                for (const EntityPropertyConstraint& constraint : nodeData->exprConstraints()) {
                    constraintExprs.push_back(constraint._expr);
                }
            }
        }
    }

    applyPredicateFilters(constraintExprs);
}

void DBProgramGenerator::applyConstraints(const VariableDependency* var) {
    const std::optional<VariableDependency::Constraint>& constraints = var->constraints();
    if (!constraints.has_value()) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const auto applyLabelConstraint = [&](const VariableDependency::LabelNames& labels) {
        const auto findIt = _part._varMap.find(var);
        const bool registered = findIt != _part._varMap.end() && !findIt->second.empty();
        bioassert(registered, "Label-constrained node not registered: {}", var->getName());
        const mlir::Value nodeColumn = findIt->second.back();

        const mlir::db::ColumnType labelSetIDType =
            allocColumnType(mlir::storage::LabelSetIDType::get(_mlirCtxt));

        const mlir::Value labelSetIDColumn = _opBuilder.create<mlir::db::GetNodeLabelSet>(
            loc,
            labelSetIDType,
            nodeColumn).getResult();

        llvm::SmallVector<llvm::StringRef> labelNames;
        for (const std::string_view label : labels) {
            labelNames.push_back(llvm::StringRef(label.data(), label.size()));
        }

        const mlir::ArrayAttr labelsAttr = _opBuilder.getStrArrayAttr(labelNames);
        const mlir::Value labelMask = _opBuilder.create<mlir::db::CheckLabelConstraint>(
            loc,
            boolType,
            labelSetIDColumn,
            labelsAttr).getResult();

        filterAllColumns(labelMask);
    };

    const auto applyEdgeTypeConstraint = [&](const VariableDependency::EdgeType& type) {
        const auto findIt = _part._edgeTypeMap.find(var);
        bioassert(findIt != _part._edgeTypeMap.end(),
                  "Type-constrained edge without a type column: {}", var->getName());
        const mlir::Value edgeTypeColumn = findIt->second;

        llvm::SmallVector<llvm::StringRef> typeNames;
        typeNames.push_back(llvm::StringRef(type.data(), type.size()));

        const mlir::ArrayAttr edgeTypesAttr = _opBuilder.getStrArrayAttr(typeNames);
        const mlir::Value edgeTypeMask = _opBuilder.create<mlir::db::CheckEdgeTypeConstraint>(
            loc,
            boolType,
            edgeTypeColumn,
            edgeTypesAttr).getResult();

        filterAllColumns(edgeTypeMask);
    };

    std::visit([&](auto&& constraint) {
        using T = std::decay_t<decltype(constraint)>;
        if constexpr (std::is_same_v<T, VariableDependency::LabelNames>) {
            applyLabelConstraint(constraint);
        } else if constexpr (std::is_same_v<T, VariableDependency::EdgeType>) {
            applyEdgeTypeConstraint(constraint);
        } else {
            // sizeof(T) == 0 (never true) keeps the assert dependent on T so it only
            // fires if this branch is ever instantiated; a bare static_assert(false)
            // is diagnosed eagerly by pre-P2593 compilers even when discarded.
            static_assert(sizeof(T) == 0, "Unhandled constraint type.");
        }
    }, *constraints);
}

void DBProgramGenerator::translateExpr(const Expr* expr) {
    if (_part._exprMap.contains(expr)) {
        return;
    }

    const Expr::Kind kind = expr->getKind();
    switch (kind) {
        case Expr::Kind::PROPERTY: {
            const PropertyExpr* propExpr = static_cast<const PropertyExpr*>(expr);
            _part._exprMap[expr] = translatePropertyExpr(propExpr);
        }
        break;

        case Expr::Kind::LITERAL: {
            const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(expr);
            _part._exprMap[expr] = translateLiteralExpr(litExpr->getLiteral());
        }
        break;

        case Expr::Kind::BINARY: {
            const BinaryExpr* binExpr = static_cast<const BinaryExpr*>(expr);
            translateBinaryExpr(expr, binExpr);
        }
        break;

        case Expr::Kind::SYMBOL: {
            const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(expr);
            const VarDecl* decl = symbolExpr->getDecl();
            const std::string_view varName = decl->getName();

            // The symbol names either a traversal variable or the alias of a projected
            // item, and no traversal ever publishes a column under an alias: the item is
            // where that column comes from
            const auto projectedIt = _part._projectedColumns.find(decl);

            if (projectedIt != end(_part._projectedColumns)) {
                _part._exprMap[expr] = projectedIt->second;
            } else {
                const mlir::Value entityColumn = resolveEntityColumn(decl);
                if (entityColumn) {
                    _part._exprMap[expr] = entityColumn;
                }
            }

            // A variable a CALL yielded appears in no pattern, so it is no VDG variable;
            // its column is the one the call produced for that return value.
            if (!_part._exprMap.contains(expr)) {
                if (const mlir::Value yielded = findYieldedColumn(decl)) {
                    _part._exprMap[expr] = yielded;
                }
            }

            const bool bound = _part._exprMap.contains(expr);

            // The row a LOAD CSV bound is not a column of its own: the load publishes one
            // per field the query reads, and nothing stands for the whole record
            const bool namesACSVRow = decl->getType() == EvaluatedType::StringTable;
            if (!bound && namesACSVRow) {
                throw TuringException(fmt::format("A CSV row cannot be read as a whole: "
                                                  "read a field of '{}' as {}[<index>] or {}.<header>",
                                                  varName, varName, varName));
            }

            bioassert(bound, "Symbol refers to unknown variable: {}", varName);
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unaryExpr = static_cast<const UnaryExpr*>(expr);
            translateUnaryExpr(expr, unaryExpr);
        }
        break;

        case Expr::Kind::STRING: {
            translateStringExpr(expr);
        }
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(expr);
            translateFunctionInvocationExpr(expr, funcExpr);
        }
        break;

        case Expr::Kind::INDEX: {
            const IndexExpr* indexExpr = static_cast<const IndexExpr*>(expr);
            const mlir::Value fieldColumn = findYieldedColumn(indexExpr->getCSVFieldDecl());

            // A load publishes one column per field its accesses named, and only a
            // constant index names one: which field a computed index reads is known no
            // earlier than the row it reads it from
            if (!fieldColumn) {
                throw TuringException("Only a constant index selects a CSV field: "
                                      "row[i] with a computed index is not supported yet.");
            }

            _part._exprMap[expr] = fieldColumn;
        }
        break;

        case Expr::Kind::LIST:
        case Expr::Kind::ENTITY_TYPES:
        case Expr::Kind::PATH:
            throw TuringException(fmt::format("Unsupported expression: {}",
                                              ExprKindDescription::value(kind)));
        break;

        case Expr::Kind::_SIZE:
            throw FatalException("Invalid expression kind.");
        break;
    }
}

void DBProgramGenerator::translateUnaryExpr(const Expr* expr, const UnaryExpr* unaryExpr) {
    const UnaryOperator op = unaryExpr->getOperator();
    const Expr* subExpr = unaryExpr->getSubExpr();
    translateExpr(subExpr);
    bioassert(_part._exprMap.contains(subExpr), "Unary operation with unknown operand.");
    const mlir::Value operandVal = _part._exprMap.at(subExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (op) {
        case UnaryOperator::Not: {
            const mlir::db::ColumnType boolType =
                allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
            auto notOp = _opBuilder.create<mlir::db::NotOp>(loc, boolType, operandVal);
            const mlir::Value result = notOp.getResult();
            _part._exprMap[expr] = result;
        }
        break;

        case UnaryOperator::Minus: {
            // -x is 0 - x, the subtraction the engine already lowers: the promotion of the
            // zero against the operand is what gives the negation its type
            const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
            const mlir::TypedAttr zeroAttr = _opBuilder.getI64IntegerAttr(0);
            const mlir::db::ColumnType zeroType = allocColumnType(zeroAttr.getType());
            const mlir::Value zero = _opBuilder.create<mlir::db::ConstantOp>(loc, zeroType, zeroAttr).getResult();

            _part._exprMap[expr] = _opBuilder.create<mlir::db::SubOp>(loc, noneType, zero, operandVal).getResult();
        }
        break;

        case UnaryOperator::Plus:
            _part._exprMap[expr] = operandVal;
        break;

        case UnaryOperator::_SIZE:
            throw TuringException("Unknown unary operator.");
        break;
    }
}

void DBProgramGenerator::translateBinaryExpr(const Expr* expr, const BinaryExpr* binExpr) {
    const Expr* lhsExpr = binExpr->getLHS();
    const Expr* rhsExpr = binExpr->getRHS();

    translateExpr(lhsExpr);
    translateExpr(rhsExpr);

    bioassert(_part._exprMap.contains(lhsExpr), "Binary operation with unknown LHS operand.");
    bioassert(_part._exprMap.contains(rhsExpr), "Binary operation with unknown RHS operand.");

    const mlir::Value lhs = _part._exprMap.at(lhsExpr);
    const mlir::Value rhs = _part._exprMap.at(rhsExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    const BinaryOperator op = binExpr->getOperator();

    switch (op) {
        case BinaryOperator::Equal:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::EqOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::And:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::AndOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Or:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::OrOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Add: {
            const EvaluatedType resultType = binExpr->getType();
            const bool isConcatenation = resultType == EvaluatedType::String || resultType == EvaluatedType::List;

            if (isConcatenation) {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::ConcatOp>(loc, noneType, lhs, rhs).getResult();
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::AddOp>(loc, noneType, lhs, rhs).getResult();
            }
        }
        break;
        case BinaryOperator::Sub:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::SubOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Mult:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::MulOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Div:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::DivOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::GreaterThan:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::GtOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::LessThan:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::LtOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::GreaterThanOrEqual:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::GteOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::LessThanOrEqual:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::LteOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::NotEqual:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::NeqOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Xor:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::XorOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Mod:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::ModOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Pow:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::PowOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::In:
            throw TuringException(fmt::format("Unsupported operation: {}",
                                              BinaryOperatorDescription::value(op)));
        break;

        case BinaryOperator::_SIZE:
        break;
    }
}

void DBProgramGenerator::translateStringExpr(const Expr* expr) {
    const StringExpr* strExpr = static_cast<const StringExpr*>(expr);

    const Expr* lhsExpr = strExpr->getLHS();
    const Expr* rhsExpr = strExpr->getRHS();

    translateExpr(lhsExpr);
    translateExpr(rhsExpr);

    bioassert(_part._exprMap.contains(lhsExpr), "String operation with unknown LHS operand.");
    bioassert(_part._exprMap.contains(rhsExpr), "String operation with unknown RHS operand.");

    const mlir::Value lhs = _part._exprMap.at(lhsExpr);
    const mlir::Value rhs = _part._exprMap.at(rhsExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const StringOperator op = strExpr->getStringOperator();

    switch (op) {
        case StringOperator::StartsWith:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::StartsWithOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case StringOperator::EndsWith:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::EndsWithOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case StringOperator::Contains:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::ContainsOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case StringOperator::_SIZE:
            throw TuringException("Unknown string operator.");
        break;
    }
}

mlir::TypedAttr DBProgramGenerator::scalarLiteralAttr(const Literal* literal) {
    switch (literal->getKind()) {
        case Literal::Kind::BOOL: {
            const BoolLiteral* boolLiteral = static_cast<const BoolLiteral*>(literal);
            return _opBuilder.getBoolAttr(boolLiteral->getValue());
        }
        break;

        case Literal::Kind::INTEGER: {
            const IntegerLiteral* intLiteral = static_cast<const IntegerLiteral*>(literal);
            return _opBuilder.getI64IntegerAttr(intLiteral->getValue());
        }
        break;

        case Literal::Kind::DOUBLE: {
            const DoubleLiteral* doubleLiteral = static_cast<const DoubleLiteral*>(literal);
            return _opBuilder.getF64FloatAttr(doubleLiteral->getValue());
        }
        break;

        case Literal::Kind::STRING: {
            const StringLiteral* stringLiteral = static_cast<const StringLiteral*>(literal);
            const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);
            return mlir::StringAttr::get(stringLiteral->getValue(), stringType);
        }
        break;

        default:
            return {};
        break;
    }
}

mlir::Attribute DBProgramGenerator::embeddingLiteralAttr(const EmbeddingLiteral* literal) {
    const std::span<const float> floats = literal->getValue();

    return mlir::DenseF32ArrayAttr::get(_mlirCtxt, llvm::ArrayRef<float> {floats.data(), floats.size()});
}

mlir::Value DBProgramGenerator::translateLiteralExpr(const Literal* literal) {
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    mlir::TypedAttr valueAttr;

    switch (literal->getKind()) {
        case Literal::Kind::BOOL:
        case Literal::Kind::INTEGER:
        case Literal::Kind::DOUBLE:
        case Literal::Kind::STRING:
            valueAttr = scalarLiteralAttr(literal);
        break;

        case Literal::Kind::EMBEDDING: {
            const mlir::Attribute embeddingAttr = embeddingLiteralAttr(static_cast<const EmbeddingLiteral*>(literal));
            const mlir::db::ColumnType embResultType = allocColumnType(mlir::storage::EmbeddingType::get(_mlirCtxt));

            return _opBuilder.create<mlir::db::ConstantOp>(uloc, embResultType, embeddingAttr).getResult();
        }
        break;

        case Literal::Kind::NULL_LITERAL: {
            const mlir::Type nullableType = mlir::storage::NullableType::get(
                _mlirCtxt, mlir::NoneType::get(_mlirCtxt));
            valueAttr = mlir::StringAttr::get("", nullableType);
        }
        break;
        case Literal::Kind::LIST:
            return translateListLiteral(static_cast<const ListLiteral*>(literal));
        break;

        default:
            throw FatalException("Unsupported literal kind in WHERE clause expression.");
        break;
    }

    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());
    return _opBuilder.create<mlir::db::ConstantOp>(uloc, resultType, valueAttr).getResult();
}

// The column of a null in every row, the shape translateLiteralExpr gives the null literal
mlir::Value DBProgramGenerator::nullConstantColumn() {
    const mlir::Type nullableType = mlir::storage::NullableType::get(_mlirCtxt,
                                                                    mlir::NoneType::get(_mlirCtxt));
    const mlir::TypedAttr valueAttr = mlir::StringAttr::get("", nullableType);
    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::translatePropertyExpr(const PropertyExpr* propExpr) {
    const VarDecl* entityDecl = propExpr->getEntityVarDecl();
    const std::string_view varName = entityDecl->getName();
    const std::string_view propName = propExpr->getPropName();

    // A header access reads a field of a loaded record rather than a property of an
    // entity: the load published its column under the declaration the access carries
    if (propExpr->isStringTableHeaderAccess()) {
        const mlir::Value fieldColumn = findYieldedColumn(propExpr->getCSVFieldDecl());
        bioassert(fieldColumn, "CSV header access on a row no load published: {}.{}", varName, propName);

        return fieldColumn;
    }

    // A created entity holds a provisional ID, which the graph a fetch reads knows nothing
    // about: the value of one of its properties is the one the CREATE wrote there, and
    // every property it did not write is null
    const auto createdIt = _part._createdEntities.find(entityDecl);
    if (createdIt != end(_part._createdEntities)) {
        const auto& properties = createdIt->second._properties;
        const auto propertyIt = properties.find(propName);

        if (propertyIt != end(properties)) {
            return propertyIt->second;
        }

        return nullConstantColumn();
    }

    const mlir::Value entityColumn = resolveEntityColumn(entityDecl);

    bioassert(entityColumn, "WHERE clause property access on unknown variable: {}", varName);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    // Use None for property type and infer during lowering
    const mlir::db::ColumnType resultType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::StringAttr propAttr = _opBuilder.getStringAttr(propName);
    const EvaluatedType entityType = entityDecl->getType();

    const bool isNode = entityType == EvaluatedType::NodePattern;
    const bool isEdge = entityType == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Property access on non-entity variable: {}", varName);

    if (isNode) {
        auto op = _opBuilder.create<mlir::db::GetNodeProperties>(loc, resultType, entityColumn, propAttr);
        return op.getResult();
    } else {
        auto op = _opBuilder.create<mlir::db::GetEdgeProperties>(loc, resultType, entityColumn, propAttr);
        return op.getResult();
    }
}

void DBProgramGenerator::translateFunctionInvocationExpr(const Expr* expr,
                                                         const FunctionInvocationExpr* funcExpr) {
    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
    const std::string_view funcName = invocation->getSignature()->getFullName();

    if (!funcExpr->isAggregate()) {
        translateFunctionExpr(expr, invocation);
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    const ExprChain* args = invocation->getArguments();
    bioassert(args && !args->empty(), "Aggregate function invocation with no arguments.");

    const Expr* argExpr = args->front();
    const mlir::Value inputColumn = translateAggregateInput(argExpr, nullptr);

    const bool isDistinct = invocation->isDistinct();

    // Dropping repeated values cannot move an extremum, so min(DISTINCT x) is min(x):
    // the flag is spent here rather than on a seen-set the result cannot depend on
    const bool isExtremum = funcName == "min" || funcName == "max";
    const bool reducesDistinctValues = isDistinct && !isExtremum;

    const bool countsRows = argExpr->getType() == EvaluatedType::Wildcard;

    if (funcName == "count") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Count>(loc, noneType, inputColumn, isDistinct, countsRows).getResult();
    } else if (funcName == "sum") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Sum>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "min") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Min>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "max") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Max>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "avg") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Avg>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "collect") {
        llvm::SmallVector<int64_t> distinctValues;
        if (reducesDistinctValues) {
            distinctValues.push_back(0);
        }

        mlir::db::Collect collectOp = createCollect({}, {inputColumn}, distinctValues);
        _part._exprMap[expr] = collectOp.getResults().back();
    } else {
        throw TuringException(fmt::format("Unsupported aggregate function: {}", funcName));
    }
}

mlir::Value DBProgramGenerator::translateArg(const Expr* argExpr) {
    translateExpr(argExpr);
    bioassert(_part._exprMap.contains(argExpr), "Function invocation with unknown argument.");
    return _part._exprMap.at(argExpr);
}

void DBProgramGenerator::translateFunctionExpr(const Expr* expr,
                                               const FunctionInvocation* invocation) {
    const std::string_view funcName = invocation->getSignature()->getFullName();
    const ExprChain* args = invocation->getArguments();

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    const auto unaryIt = unaryFunctionEmitters.find(funcName);
    if (unaryIt != end(unaryFunctionEmitters)) {
        if (!args || args->size() != 1) {
            throw TuringException(fmt::format("{}() expects 1 argument.", funcName));
        }

        const mlir::Value input = translateArg(args->front());
        _part._exprMap[expr] = unaryIt->second(_opBuilder, loc, noneType, input);
        return;
    }

    const auto binaryIt = binaryFunctionEmitters.find(funcName);
    if (binaryIt != end(binaryFunctionEmitters)) {
        if (!args || args->size() != 2) {
            throw TuringException(fmt::format("{}() expects 2 arguments.", funcName));
        }

        const mlir::Value lhs = translateArg(args->getExprs()[0]);
        const mlir::Value rhs = translateArg(args->getExprs()[1]);
        _part._exprMap[expr] = binaryIt->second(_opBuilder, loc, noneType, lhs, rhs);
        return;
    }

    throw TuringException(fmt::format("Unsupported function: {}", funcName));
}

mlir::db::Collect DBProgramGenerator::createCollect(llvm::ArrayRef<mlir::Value> keyColumns,
                                                    llvm::ArrayRef<mlir::Value> valueColumns,
                                                    llvm::ArrayRef<int64_t> distinctValues,
                                                    llvm::ArrayRef<mlir::Value> aggregateColumns,
                                                    llvm::ArrayRef<mlir::storage::GroupAggregateKind> aggregateKinds) {
    llvm::SmallVector<mlir::Value> columns(keyColumns.begin(), keyColumns.end());
    columns.append(valueColumns.begin(), valueColumns.end());
    columns.append(aggregateColumns.begin(), aggregateColumns.end());

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value keyColumn : keyColumns) {
        resultTypes.push_back(keyColumn.getType());
    }

    for (const mlir::Value valueColumn : valueColumns) {
        const mlir::db::ColumnType valueType = mlir::cast<mlir::db::ColumnType>(valueColumn.getType());
        const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, valueType.getType());

        resultTypes.push_back(allocColumnType(listType));
    }

    // An aggregate's result type is resolved during lowering, as db.group_aggregate's is.
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    llvm::SmallVector<int64_t> kindValues;
    for (const mlir::storage::GroupAggregateKind kind : aggregateKinds) {
        kindValues.push_back(static_cast<int64_t>(kind));
        resultTypes.push_back(noneType);
    }

    const mlir::DenseI64ArrayAttr kindsAttr = kindValues.empty()
                                                  ? mlir::DenseI64ArrayAttr {}
                                                  : _opBuilder.getDenseI64ArrayAttr(kindValues);

    const mlir::DenseI64ArrayAttr distinctAttr = distinctValues.empty()
                                                     ? mlir::DenseI64ArrayAttr {}
                                                     : _opBuilder.getDenseI64ArrayAttr(distinctValues);

    return _opBuilder.create<mlir::db::Collect>(_opBuilder.getUnknownLoc(),
                                                mlir::TypeRange {resultTypes},
                                                mlir::ValueRange {columns},
                                                static_cast<uint64_t>(keyColumns.size()),
                                                kindsAttr,
                                                distinctAttr);
}

void DBProgramGenerator::generateKeylessCollect(const Projection* projection) {
    llvm::SmallVector<const FunctionInvocationExpr*> collectExprs;
    collectProjectedCollects(projection, collectExprs);

    // A lone collect is built by its own translation, keyless, as any other aggregate is
    if (collectExprs.size() < 2) {
        return;
    }

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    llvm::SmallVector<mlir::Value> valueColumns;
    for (const FunctionInvocationExpr* collectExpr : collectExprs) {
        const FunctionInvocation* invocation = collectExpr->getFunctionInvocation();

        const ExprChain* args = invocation->getArguments();
        bioassert(args && !args->empty(), "collect() with no arguments.");

        valueColumns.push_back(translateAggregateInput(args->front(), &variableColumns));
    }

    llvm::SmallVector<int64_t> distinctValues;
    collectDistinctValueIndices(collectExprs, distinctValues);

    mlir::db::Collect collectOp = createCollect({}, valueColumns, distinctValues);
    const mlir::ResultRange results = collectOp.getResults();

    for (size_t collectIndex = 0; collectIndex < collectExprs.size(); collectIndex++) {
        const FunctionInvocationExpr* collectExpr = collectExprs[collectIndex];
        const mlir::Value listColumn = results[collectIndex];

        _part._exprMap[collectExpr] = listColumn;

        // The alias of a collect names its list, so a later item spelling that alias
        // reads this column instead of collecting a second time
        const VarDecl* collectDecl = collectExpr->getExprVarDecl();
        if (collectDecl) {
            _part._projectedColumns[collectDecl] = listColumn;
        }
    }
}

void DBProgramGenerator::generateGroupAggregate(const Projection* projection) {
    if (!projection->isAggregate()) {
        return;
    }

    if (!projection->hasGroupingKeys()) {
        generateKeylessCollect(projection);
        return;
    }

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    // An edge identity is carried by the traversal variable that produced it, under a
    // declaration of its own: the representative is where its column - and its grouped
    // replacement - is published, since the identity has no variable of its own
    std::unordered_map<const VarDecl*, const VariableDependency*> edgeIdentityVars;
    for (const auto& [decl, vars] : _vdg.edgeIdentities()) {
        if (!vars.empty() && _part._varMap.contains(vars.front())) {
            edgeIdentityVars[decl] = vars.front();
        }
    }

    // Parallel: a key position is either a variable, declared by keyVarDeclAtPos and
    // carried by keyVarAtPos, or an expression, held by keyExprAtPos. A column a CALL
    // yielded is a variable with no keyVarAtPos: it appears in no pattern, so the VDG has
    // nothing for it and its declaration is the only handle on it.
    llvm::SmallVector<mlir::Value> keyColumns;
    llvm::SmallVector<const VariableDependency*> keyVarAtPos;
    llvm::SmallVector<const VarDecl*> keyVarDeclAtPos;
    llvm::SmallVector<const Expr*> keyExprAtPos;

    llvm::SmallVector<mlir::Value> aggInputColumns;
    llvm::SmallVector<mlir::storage::GroupAggregateKind> aggKinds;
    llvm::SmallVector<const FunctionInvocationExpr*> aggFuncExprs;
    llvm::SmallVector<const FunctionInvocationExpr*> itemInvocations;

    llvm::SmallVector<mlir::Value> collectInputColumns;
    llvm::SmallVector<const FunctionInvocationExpr*> collectFuncExprs;

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (const Projection::ReturnItem& returnItem : projection->items()) {
        if (const VarDecl* const* varDeclPtr = std::get_if<VarDecl*>(&returnItem)) {
            const VarDecl* decl = *varDeclPtr;
            const auto findIt = variableColumns.find(decl);
            bioassert(findIt != variableColumns.end(), "Grouping key variable {} not found.", decl->getName());
            const mlir::Value keyColumn = findIt->second;

            // A constant tells no two rows apart, so it groups nothing - whether the
            // projection spells it out or a wildcard expands it
            if (yieldsConstantColumn(keyColumn)) {
                continue;
            }

            keyColumns.push_back(keyColumn);

            // An edge identity's column is published under its representative, and that
            // is the one the projection reads back, so the grouped column has to replace
            // it there
            const VariableDependency* keyVar = nullptr;
            const auto identityIt = edgeIdentityVars.find(decl);

            if (identityIt != edgeIdentityVars.end()) {
                keyVar = identityIt->second;
            } else {
                for (auto& [var, values] : _part._varMap) {
                    if (var->getDecl() == decl) {
                        keyVar = var;
                        break;
                    }
                }
            }

            keyVarAtPos.push_back(keyVar);
            keyVarDeclAtPos.push_back(decl);
            keyExprAtPos.push_back(nullptr);
            continue;
        }

        const Expr* item = std::get<Expr*>(returnItem);

        if (!item->isAggregate()) {
            const mlir::Value keyColumn = getOrTranslateExprColumn(variableColumns, item);

            // An aggregate may be taken over the alias of an item declared before it -
            // count(x) of RETURN 1 AS x, count(x) - and the column that alias names is
            // this one, so it is published under the item's declaration for the symbol
            // to be resolved through, as the projection publishes it for an ORDER BY key
            const VarDecl* itemDecl = item->getExprVarDecl();
            if (itemDecl) {
                _part._projectedColumns[itemDecl] = keyColumn;
            }

            // A constant column holds the same value in every row, so it tells no two
            // rows apart: it groups nothing and rides along beside the groups, as it
            // rides past a dedup or a sort
            if (yieldsConstantColumn(keyColumn)) {
                continue;
            }

            keyColumns.push_back(keyColumn);
            keyVarAtPos.push_back(nullptr);
            keyVarDeclAtPos.push_back(nullptr);
            keyExprAtPos.push_back(item);
            continue;
        }

        // An item may carry an aggregate without being one: 2 * count(n) + 20 is an
        // arithmetic expression the group reduces a count for, not an aggregate function
        // it reduces whole. Its aggregates become the reduced columns, and the arithmetic
        // around them is left for the projection to compute over the results.
        if (!collectAggregateInvocations(item, itemInvocations)) {
            const std::string_view itemName = item->getName();
            throw TuringException(fmt::format("Nested aggregates are not supported: {}", itemName));
        }
    }

    // Every non-aggregate item was constant, so nothing tells the matched rows apart: the
    // projection is the one keyless group, whichever way it spells its key - RETURN 1 AS
    // x, count(n) counts the whole match, as RETURN count(n) does
    if (keyColumns.empty()) {
        generateKeylessCollect(projection);
        return;
    }

    // A key may order the groups by an aggregate the projection does not return -
    // RETURN a.name ORDER BY count(b) - which the aggregation has to compute all the same.
    // Its result column is then read by the sort alone, and no db.output reads it, which is
    // what keeps it out of the rows
    if (projection->hasOrderBy()) {
        llvm::SmallVector<const FunctionInvocationExpr*> keyInvocations;
        for (const OrderByItem* orderByItem : projection->getOrderBy()->getItems()) {
            collectAggregateInvocations(orderByItem->getExpr(), keyInvocations);
        }

        for (const FunctionInvocationExpr* keyInvocation : keyInvocations) {
            const bool alreadyReduced = std::any_of(itemInvocations.begin(),
                                                    itemInvocations.end(),
                                                    [keyInvocation](const FunctionInvocationExpr* collected) {
                                                        return StructuralExpressionComparator::equal(collected, keyInvocation);
                                                    });

            if (!alreadyReduced) {
                itemInvocations.push_back(keyInvocation);
            }
        }
    }

    for (const FunctionInvocationExpr* funcExpr : itemInvocations) {
        const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
        const std::string_view funcName = invocation->getSignature()->getFullName();

        const ExprChain* args = invocation->getArguments();
        bioassert(args && !args->empty(), "Aggregate function invocation with no arguments.");

        const Expr* argExpr = args->front();

        if (funcName == "collect") {
            const mlir::Value collectInput = translateAggregateInput(argExpr, &variableColumns);

            collectInputColumns.push_back(collectInput);
            collectFuncExprs.push_back(funcExpr);
            continue;
        }

        // Dropping repeated values cannot move an extremum, so min(DISTINCT x) is min(x)
        // and takes the plain kind. The others reduce each of a group's distinct values
        // once, which is a kind of its own, spelled as the function's name with the
        // modifier appended
        const bool isExtremum = funcName == "min" || funcName == "max";
        const bool reducesDistinctValues = invocation->isDistinct() && !isExtremum;

        const bool countsRows = argExpr->getType() == EvaluatedType::Wildcard;

        // count(*) reads no value, so a null of the column it is anchored on is a row
        // all the same: that is a kind of its own
        std::string kindName {funcName};
        if (countsRows) {
            kindName += "_rows";
        } else if (reducesDistinctValues) {
            kindName += "_distinct";
        }

        const std::optional<mlir::storage::GroupAggregateKind> kind = mlir::storage::symbolizeGroupAggregateKind(kindName);
        if (!kind) {
            throw TuringException(fmt::format("Unsupported aggregate function: {}", funcName));
        }

        const mlir::Value inputColumn = translateAggregateInput(argExpr, &variableColumns);

        aggInputColumns.push_back(inputColumn);
        aggKinds.push_back(*kind);
        aggFuncExprs.push_back(funcExpr);
    }

    const size_t keyCount = keyColumns.size();
    const size_t aggCount = aggInputColumns.size();
    const size_t collectCount = collectInputColumns.size();
    bioassert(aggCount + collectCount > 0, "grouped aggregate with no aggregate columns.");

    const bool isCollecting = collectCount > 0;

    mlir::Operation* aggregateOp = nullptr;

    if (isCollecting) {
        // One accumulator holds the groups every list and every reduction reads, so the
        // collects carry each other and the other aggregates rather than a second op
        // grouping the same rows again.
        llvm::SmallVector<int64_t> distinctCollects;
        collectDistinctValueIndices(collectFuncExprs, distinctCollects);

        mlir::db::Collect collectOp = createCollect(keyColumns,
                                                    collectInputColumns,
                                                    distinctCollects,
                                                    aggInputColumns,
                                                    aggKinds);
        aggregateOp = collectOp.getOperation();
    } else {
        llvm::SmallVector<mlir::Value> allColumns;
        for (const mlir::Value col : keyColumns) {
            allColumns.push_back(col);
        }
        for (const mlir::Value col : aggInputColumns) {
            allColumns.push_back(col);
        }

        llvm::SmallVector<mlir::Type> resultTypes;
        for (const mlir::Value col : keyColumns) {
            resultTypes.push_back(col.getType());
        }
        for (size_t i = 0; i < aggCount; i++) {
            resultTypes.push_back(noneType);
        }

        llvm::SmallVector<int64_t> aggKindValues;
        for (const mlir::storage::GroupAggregateKind kind : aggKinds) {
            aggKindValues.push_back(static_cast<int64_t>(kind));
        }

        mlir::db::GroupAggregate groupAgg = _opBuilder.create<mlir::db::GroupAggregate>(
            loc,
            mlir::TypeRange{resultTypes},
            mlir::ValueRange{allColumns},
            static_cast<uint64_t>(keyCount),
            llvm::ArrayRef<int64_t>{aggKindValues});

        aggregateOp = groupAgg.getOperation();
    }

    const mlir::ResultRange results = aggregateOp->getResults();

    GroupedColumns groupedColumns;

    for (size_t i = 0; i < keyCount; i++) {
        if (keyVarDeclAtPos[i]) {
            if (keyVarAtPos[i]) {
                registerValue(keyVarAtPos[i], results[i]);
            }

            // A YIELD can bind onto a variable a pattern already carries, which leaves the
            // one declaration on both a variable and a yielded column, so both are rebound.
            rebindYieldedColumn(keyVarDeclAtPos[i], results[i]);
            continue;
        }

        _part._exprMap[keyExprAtPos[i]] = results[i];
        groupedColumns.emplace_back(keyExprAtPos[i], results[i]);

        const bool isSymbol = keyExprAtPos[i]->getKind() == Expr::Kind::SYMBOL;
        if (not isSymbol) {
            continue;
        }

        // Symbols need their value updated: the aggregate gives them a new value.
        // An edge variable holds an identity rather than a traversal variable of its
        // own, and the projection reads it back through the identity's
        // representative, so that is where the grouped column has to land
        const SymbolExpr* sym = static_cast<const SymbolExpr*>(keyExprAtPos[i]);
        const VarDecl* symDecl = sym->getDecl();

        rebindYieldedColumn(symDecl, results[i]);

        const auto identityIt = edgeIdentityVars.find(symDecl);

        if (identityIt != edgeIdentityVars.end()) {
            registerValue(identityIt->second, results[i]);
            continue;
        }

        for (auto& [var, values] : _part._varMap) {
            if (var->getDecl() == symDecl) {
                registerValue(var, results[i]);
                break;
            }
        }
    }

    // The results behind the keys are the collects' lists then the reductions, or - with
    // no collect - the reductions alone.
    llvm::SmallVector<const FunctionInvocationExpr*> aggregateItems(collectFuncExprs.begin(),
                                                                    collectFuncExprs.end());
    aggregateItems.append(aggFuncExprs.begin(), aggFuncExprs.end());

    for (size_t i = 0; i < aggregateItems.size(); i++) {
        const FunctionInvocationExpr* aggregateItem = aggregateItems[i];
        const mlir::Value aggregateColumn = results[keyCount + i];

        _part._exprMap[aggregateItem] = aggregateColumn;
        groupedColumns.emplace_back(aggregateItem, aggregateColumn);

        // The alias of an aggregate names its reduced column, so a key or a later item
        // spelling that alias reads this column instead of computing a second aggregate
        const VarDecl* aggregateDecl = aggregateItem->getExprVarDecl();
        if (aggregateDecl) {
            _part._projectedColumns[aggregateDecl] = aggregateColumn;
        }
    }

    bindOrderByKeyColumns(projection, groupedColumns);
}

void DBProgramGenerator::bindOrderByKeyColumns(const Projection* projection,
                                               const GroupedColumns& groupedColumns) {
    if (!projection->hasOrderBy() || groupedColumns.empty()) {
        return;
    }

    const OrderBy* orderBy = projection->getOrderBy();

    for (const OrderByItem* item : orderBy->getItems()) {
        bindGroupedKeyColumn(item->getExpr(), groupedColumns);
    }
}

void DBProgramGenerator::bindGroupedKeyColumn(const Expr* expr, const GroupedColumns& groupedColumns) {
    if (!expr || _part._exprMap.contains(expr)) {
        return;
    }

    for (const auto& [keyExpr, keyColumn] : groupedColumns) {
        // The key reads this grouping key whole, so the grouped column is what it reads:
        // nothing below it has to be looked at, let alone translated again
        if (StructuralExpressionComparator::equal(keyExpr, expr)) {
            _part._exprMap[expr] = keyColumn;
            return;
        }
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return;
    }

    for (const Expr* child : children) {
        bindGroupedKeyColumn(child, groupedColumns);
    }
}

bool DBProgramGenerator::collectAggregateInvocations(const Expr* expr,
                                                     llvm::SmallVectorImpl<const FunctionInvocationExpr*>& found) {
    if (!expr->isAggregate()) {
        return true;
    }

    if (expr->getKind() == Expr::Kind::FUNCTION_INVOCATION) {
        const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(expr);
        const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();

        // An aggregate over an aggregate is invalid Cypher the analyzer turns away, so a
        // reduced call is a leaf: its argument is the group's input, not another aggregate
        if (invocation->getSignature()->isAggregate()) {
            found.push_back(funcExpr);
            return true;
        }
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return false;
    }

    for (const Expr* child : children) {
        if (!collectAggregateInvocations(child, found)) {
            return false;
        }
    }

    return true;
}
