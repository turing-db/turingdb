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
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "DBTypes.h"
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
#include "stmt/MatchStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/ReturnStmt.h"
#include "stmt/SetItem.h"
#include "stmt/SetStmt.h"
#include "stmt/Skip.h"
#include "stmt/StmtContainer.h"
#include "stmt/UnwindStmt.h"
#include "Symbol.h"
#include "SymbolChain.h"

#include "columns/BinaryOperators.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace db;

namespace {

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

// The projected columns that carry rows, and the item each of them is. A constant column
// holds the same value in every row, so a step shaped by rows - a dedup, a sort, a cut -
// is not given one: it rides along untouched, and reading it would anchor that step where
// the constant is bound, above the loop the other columns are read in
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
    _varMap[var].emplace_back(val);
}

void DBProgramGenerator::addScanNodes(const VariableDependency* var) {
    bioassert(!_varMap.contains(var), "ScanNodes for registered variable");

    const auto col = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    auto scan = _opBuilder.create<mlir::db::ScanNodes>(_opBuilder.getUnknownLoc(), col);

    registerValue(var, scan.getResult());
}

void DBProgramGenerator::addUnwindConst(const VariableDependency* var, const UnwindStmt* unwind) {
    bioassert(!_varMap.contains(var), "UnwindConst for registered variable");

    // The analyzer restricts UNWIND to a literal list, so anything else here is a
    // statement it let through and this path cannot lower.
    const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(unwind->arg());
    if (!literalExpr) {
        throw TuringException("Non-literal UNWIND expressions are not yet supported.");
    }

    const ListLiteral* list = dynamic_cast<const ListLiteral*>(literalExpr->getLiteral());
    if (!list) {
        throw TuringException("Non-list arguments to UNWIND are not yet supported.");
    }

    llvm::SmallVector<mlir::Attribute> elements;
    translateListElements(list, elements);

    // The result element type is the homogeneity verdict db.unwind_const reads: elements
    // sharing one type unwind into a column of that type, and a mixed - or empty - list
    // into a type-erased column of tagged scalars.
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

mlir::Attribute DBProgramGenerator::listElementAttr(const Literal* literal) {
    const Literal::Kind kind = literal->getKind();

    if (kind == Literal::Kind::NULL_LITERAL) {
        return _opBuilder.getUnitAttr();
    } else if (kind == Literal::Kind::LIST) {
        const ListLiteral* nested = static_cast<const ListLiteral*>(literal);

        llvm::SmallVector<mlir::Attribute> nestedElements;
        translateListElements(nested, nestedElements);

        return _opBuilder.getArrayAttr(nestedElements);
    }

    const mlir::TypedAttr scalarAttr = scalarLiteralAttr(literal);
    if (!scalarAttr) {
        throw TuringException("Only booleans, integers, floats, strings, nulls and lists are "
                              "supported as list elements.");
    }

    return scalarAttr;
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
    static_assert(std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetInEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetEdges>, "Invalid op");

    bioassert(src, "Null source");
    bioassert(tgt, "Null target");

    bioassert(_varMap.contains(src), "Edge traversal without source");

    const auto srcs = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto eids = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const auto etypes = allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));
    const auto tgts = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));

    const mlir::Value input = _varMap[src].back();

    llvm::SmallVector<const VariableDependency*> carried;
    llvm::SmallVector<mlir::Value> operands {input};
    llvm::SmallVector<mlir::Type> results {srcs, eids, etypes, tgts};
    for (const VariableDependency* var : carrySet) {
        // source variable is explicitly filtered by the edge op
        if (var == src) {
            continue;
        }

        const mlir::Value column = _varMap[var].back();
        carried.push_back(var);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    // Find the edge types to carry which were defined in this block
    mlir::Block* const insertionBlock = _opBuilder.getInsertionBlock();
    llvm::SmallVector<const VariableDependency*> carriedEdgeTypes;
    for (auto& [edgeVar, column] : _edgeTypeMap) {
        mlir::Operation* const definingOp = column.getDefiningOp();
        mlir::Block* const definingBlock = definingOp
            ? definingOp->getBlock()
            : mlir::cast<mlir::BlockArgument>(column).getOwner();
        if (definingBlock != insertionBlock) {
            continue;
        }
        carriedEdgeTypes.push_back(edgeVar);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    const auto loc = _opBuilder.getUnknownLoc();
    auto op = _opBuilder.create<EdgeOp>(loc, results, operands);

    const mlir::Value newSrcs = op.getResult(0);
    const mlir::Value newEdges = op.getResult(1);
    const mlir::Value newEtypes = op.getResult(2);
    const mlir::Value newTgts = op.getResult(3);

    _edgeTypeMap[edge] = newEtypes;

    constexpr bool forwardOrientation = std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                                        or std::is_same_v<EdgeOp, mlir::db::GetEdges>;
    if constexpr (forwardOrientation) {
        registerValue(src, newSrcs);
        registerValue(tgt, newTgts);
    } else {
        registerValue(src, newTgts);
        registerValue(tgt, newSrcs);
    }
    registerValue(edge, newEdges);

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
        _edgeTypeMap[carriedEdgeTypes[i]] = op.getResult(edgeTypeOffset + i);
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

    generateTraversal(ast);
    resolveEdgeIdentities();
    generatePropertyConstraints(ast);
    generateFilters(ast);
    generateCalls(ast);
    generateGroupAggregate(ast);
    generateCreate(ast);
    generateSet(ast);
    generateDelete(ast);
    generateOutput(ast);

    _opBuilder.create<mlir::func::ReturnOp>(uloc);

    runPasses();
}

void DBProgramGenerator::runPasses() {
    mlir::PassManager passManager(_mlirCtxt);
    passManager.addPass(mlir::db::createFuseScanByLabel());

    if (mlir::failed(passManager.run(*_module))) {
        throw FatalException("DB pass pipeline failed");
    }
}

void DBProgramGenerator::generateTraversal(const CypherAST* ast) {
    _vdg.buildFromAST(ast);

    if (_vdg.empty()) {
        return;
    }

    // Main block is saved so we can splice into it after generation
    mlir::Block* const mainBlock = _opBuilder.getInsertionBlock();

    DefinedVars defined;

    // Connected components
    std::vector<TranslatedComponent> components;

    // TODO: Use nodes at ends of diameter
    for (const VariableDependency& root : _vdg.vars()) {
        if (defined.contains(&root)) {
            continue;
        }

        const auto isEdgeTgtMetaVar = [](const DependencyEdge* e) -> bool {
            return e->isMetaEdge();
        };

        // A valid root is a non-meta Cypher variable which is a node
        const bool validRoot =
            std::ranges::none_of(root.incoming(), [&](const DependencyEdge* e) {
                return producesEdgeVar(e) || isEdgeTgtMetaVar(e);
            });

        if (!validRoot) {
            continue;
        }

        TranslatedComponent& component = components.emplace_back();
        component._region = std::make_unique<mlir::Region>();

        mlir::Block* const scratch = new mlir::Block();
        component._region->push_back(scratch); // Region destructor frees scratch
        _opBuilder.setInsertionPointToStart(scratch);

        translateComponent(&root, defined, component._vars);

        for (const VariableDependency* var : component._vars) {
            bioassert(_varMap.contains(var), "Component var {} not registered", var->getName());
            component._columns.push_back(_varMap[var].back());
        }
    }

    if (components.empty()) {
        return;
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

void DBProgramGenerator::collectLiveColumns(LiveColumns& live) {
    // Only columns defined in the current insertion block are live to an op placed here
    mlir::Block* const insertionBlock = _opBuilder.getInsertionBlock();

    const auto isLive = [&](const mlir::Value column) {
        mlir::Operation* const definingOp = column.getDefiningOp();
        mlir::Block* const definingBlock = definingOp
            ? definingOp->getBlock()
            : mlir::cast<mlir::BlockArgument>(column).getOwner();

        return definingBlock == insertionBlock;
    };

    for (auto& [var, values] : _varMap) {
        const mlir::Value column = values.back();
        if (!isLive(column) || _deadVariables.contains(var)) {
            continue;
        }

        live._columns.push_back(column);
        live._variables.push_back(var);
    }

    for (auto& [var, column] : _edgeTypeMap) {
        if (!isLive(column) || _deadVariables.contains(var)) {
            continue;
        }

        live._columns.push_back(column);
        live._edgeTypeVariables.push_back(var);
    }

    // A column an earlier CALL yielded is live too: a later op taking the whole row set
    // must take it along, or the rows it holds would stop matching the ones beside it.
    for (size_t yieldedIndex = 0; yieldedIndex < _yieldedColumns.size(); yieldedIndex++) {
        const mlir::Value column = _yieldedColumns[yieldedIndex].second;
        if (!isLive(column) || _deadYieldedColumns.contains(yieldedIndex)) {
            continue;
        }

        live._columns.push_back(column);
        live._yieldedIndices.push_back(yieldedIndex);
    }
}

void DBProgramGenerator::rebindLiveColumns(mlir::Operation::result_range results,
                                           size_t firstResult,
                                           const LiveColumns& live) {
    size_t resultIndex = firstResult;

    for (const VariableDependency* variable : live._variables) {
        registerValue(variable, results[resultIndex]);
        resultIndex++;
    }

    for (const VariableDependency* variable : live._edgeTypeVariables) {
        _edgeTypeMap[variable] = results[resultIndex];
        resultIndex++;
    }

    for (const size_t yieldedIndex : live._yieldedIndices) {
        _yieldedColumns[yieldedIndex].second = results[resultIndex];
        resultIndex++;
    }
}

mlir::Value DBProgramGenerator::findYieldedColumn(std::string_view name) const {
    for (const YieldedColumn& yielded : _yieldedColumns) {
        if (yielded.first == name) {
            return yielded.second;
        }
    }

    return mlir::Value();
}

void DBProgramGenerator::filterAllColumns(mlir::Value predicate) {
    LiveColumns live;
    collectLiveColumns(live);

    if (live._columns.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : live._columns) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto filterOp = _opBuilder.create<mlir::db::FilterOp>(loc, resultTypes, predicate, live._columns);

    rebindLiveColumns(filterOp.getResults(), /*firstResult=*/0, live);
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
    const mlir::Value fstSourceCol = _varMap.at(fstMergeSource).back();
    const mlir::Value sndSourceCol = _varMap.at(sndMergeSource).back();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    // Create an EQ op to keep only rows where both sources are the same
    auto eq =
        _opBuilder.create<mlir::db::EqOp>(uloc, boolType, fstSourceCol, sndSourceCol);
    const mlir::Value eqRes = eq.getResult();

    // Register mergeVar's initial value (first source, arbitrary) so filterAllColumns
    // picks it up along with the rest of _varMap.
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
            const mlir::Value fstCol = findVarOrThrow(_varMap, fstVar);
            const mlir::Value sndCol = findVarOrThrow(_varMap, sndVar);

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

void DBProgramGenerator::translateComponent(const VariableDependency* root,
                                            DefinedVars& defined,
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

    // A root either is a pattern variable, whose dataflow opens with a node scan, or is
    // bound by an UNWIND, whose dataflow opens with the literal list itself.
    const VariableDependencyGraph::UnwindSourceMap& unwindSources = _vdg.unwindSources();
    const auto unwindIt = unwindSources.find(root);

    if (unwindIt != unwindSources.end()) {
        addUnwindConst(root, unwindIt->second);
    } else {
        addScanNodes(root);
    }

    markDefined(root);
    applyConstraints(root);

    // Forms the "carried set" for this connected component
    std::vector<const VariableDependency*> carriedSet;

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

void DBProgramGenerator::generateCalls(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const StmtContainer* stmtsContainer = sglPart->getReadStmts();
    if (!stmtsContainer) {
        return;
    }

    // In statement order, so a call reading what an earlier one yielded sees it - the
    // reading statements are generated in the order the query writes them. Gathered first
    // because each call is told what the calls after it still read.
    llvm::SmallVector<const CallStmt*> calls;
    for (const Stmt* stmt : stmtsContainer->stmts()) {
        if (stmt->getKind() != Stmt::Kind::CALL) {
            continue;
        }

        calls.push_back(static_cast<const CallStmt*>(stmt));
    }

    const ReturnStmt* returnStmt = sglPart->getReturnStmt();

    for (size_t callIndex = 0; callIndex < calls.size(); callIndex++) {
        const CallStmt* call = calls[callIndex];
        const llvm::ArrayRef<const CallStmt*> laterCalls = llvm::ArrayRef(calls).drop_front(callIndex + 1);

        VariableNames usedAfterCall;
        collectNamesUsedAfterCall(call, laterCalls, returnStmt, usedAfterCall);

        generateCall(call, usedAfterCall);
    }
}

void DBProgramGenerator::collectNamesUsedAfterCall(const CallStmt* call,
                                                   llvm::ArrayRef<const CallStmt*> laterCalls,
                                                   const ReturnStmt* returnStmt,
                                                   VariableNames& used) const {
    // This call's own YIELD ... WHERE filters the rows it emitted, so it runs after the
    // call and its variables have to survive it. Its arguments do not: they are operands of
    // the call, read where it sits rather than past it.
    collectYieldWhereNames(call, used);

    // A later call reads the variables its own arguments are built from, and the ones its
    // YIELD ... WHERE filters on.
    for (const CallStmt* laterCall : laterCalls) {
        collectYieldWhereNames(laterCall, used);

        const FunctionInvocationExpr* funcExpr = laterCall->getFunc();
        const FunctionInvocation* invocation = funcExpr ? funcExpr->getFunctionInvocation() : nullptr;
        const ExprChain* arguments = invocation ? invocation->getArguments() : nullptr;
        if (!arguments) {
            continue;
        }

        for (const Expr* argument : *arguments) {
            collectExprNames(argument, used);
        }
    }

    // With no projection the query emits the columns the calls yielded, in yield order,
    // and nothing besides - so every yielded variable is read and no pattern variable is.
    if (!returnStmt) {
        for (const YieldedColumn& yieldedColumn : _yieldedColumns) {
            used.insert(yieldedColumn.first);
        }

        return;
    }

    const Projection* projection = returnStmt->getProjection();
    if (!projection) {
        return;
    }

    // RETURN * names no variable of its own, so anything in scope may be projected and
    // none of it can be called dead.
    if (projection->isReturningAll()) {
        for (const auto& [variable, columns] : _varMap) {
            used.insert(variable->getName());
        }

        for (const YieldedColumn& yieldedColumn : _yieldedColumns) {
            used.insert(yieldedColumn.first);
        }

        return;
    }

    const auto collectItemNames = [&](auto&& item) {
        using Type = std::remove_cvref_t<decltype(item)>;

        if constexpr (std::is_same_v<Type, VarDecl*>) {
            used.insert(item->getName());
        } else {
            collectExprNames(item, used);
        }
    };

    for (const Projection::ReturnItem item : projection->items()) {
        std::visit(collectItemNames, item);
    }

    // An ORDER BY reads its keys after the call as much as the projection does.
    if (projection->hasOrderBy()) {
        const OrderBy* orderBy = projection->getOrderBy();
        for (const OrderByItem* item : orderBy->getItems()) {
            collectExprNames(item->getExpr(), used);
        }
    }
}

void DBProgramGenerator::collectYieldWhereNames(const CallStmt* call, VariableNames& used) const {
    const YieldClause* yield = call->getYield();
    const YieldItems* yieldItems = yield ? yield->getItems() : nullptr;
    const WhereClause* where = yieldItems ? yieldItems->getWhereClause() : nullptr;
    if (!where) {
        return;
    }

    collectExprNames(where->getExpr(), used);
}

void DBProgramGenerator::collectExprNames(const Expr* expr, VariableNames& used) const {
    if (!expr) {
        return;
    }

    // The variable an expression is attached to, which is what the projection resolves it
    // by before it translates the expression at all.
    const VarDecl* exprVar = expr->getExprVarDecl();
    if (exprVar) {
        used.insert(exprVar->getName());
    }

    const Expr::Kind kind = expr->getKind();
    switch (kind) {
        case Expr::Kind::SYMBOL: {
            const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(expr);
            const VarDecl* decl = symbolExpr->getDecl();
            if (decl) {
                used.insert(decl->getName());
            }
        }
        break;

        case Expr::Kind::PROPERTY: {
            const PropertyExpr* propertyExpr = static_cast<const PropertyExpr*>(expr);
            const VarDecl* entityDecl = propertyExpr->getEntityVarDecl();
            if (entityDecl) {
                used.insert(entityDecl->getName());
            }
        }
        break;

        case Expr::Kind::ENTITY_TYPES: {
            const EntityTypeExpr* entityTypeExpr = static_cast<const EntityTypeExpr*>(expr);
            const VarDecl* entityDecl = entityTypeExpr->getEntityVarDecl();
            if (entityDecl) {
                used.insert(entityDecl->getName());
            }
        }
        break;

        case Expr::Kind::BINARY: {
            const BinaryExpr* binaryExpr = static_cast<const BinaryExpr*>(expr);
            collectExprNames(binaryExpr->getLHS(), used);
            collectExprNames(binaryExpr->getRHS(), used);
        }
        break;

        case Expr::Kind::STRING: {
            const StringExpr* stringExpr = static_cast<const StringExpr*>(expr);
            collectExprNames(stringExpr->getLHS(), used);
            collectExprNames(stringExpr->getRHS(), used);
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unaryExpr = static_cast<const UnaryExpr*>(expr);
            collectExprNames(unaryExpr->getSubExpr(), used);
        }
        break;

        case Expr::Kind::INDEX: {
            const IndexExpr* indexExpr = static_cast<const IndexExpr*>(expr);
            collectExprNames(indexExpr->getBase(), used);
            collectExprNames(indexExpr->getIndexExpr(), used);
        }
        break;

        case Expr::Kind::LIST: {
            const ListExpr* listExpr = static_cast<const ListExpr*>(expr);
            for (const Expr* element : *listExpr) {
                collectExprNames(element, used);
            }
        }
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(expr);
            const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
            const ExprChain* arguments = invocation ? invocation->getArguments() : nullptr;
            if (arguments) {
                for (const Expr* argument : *arguments) {
                    collectExprNames(argument, used);
                }
            }
        }
        break;

        case Expr::Kind::PATH:
        case Expr::Kind::LITERAL:
            // A literal reads no variable, and a path is attached to its own through the
            // declaration read above.
        break;

        case Expr::Kind::_SIZE:
            throw FatalException("Invalid expression kind.");
        break;
    }
}

void DBProgramGenerator::dropUnusedLiveColumns(const VariableNames& usedAfterCall, LiveColumns& live) {
    LiveColumns usedColumns;

    // The three groups sit back to back in _columns, in this order, which is the order
    // collectLiveColumns filled them and rebindLiveColumns walks them - so one running
    // index over _columns tracks all three.
    size_t columnIndex = 0;

    // A column left out here stops flowing with the rows, so it is marked dead: its last
    // value still sits in _varMap or _yieldedColumns but belongs to the loop the call is
    // about to leave, and a later op collecting everything in flight must not pick it up.
    for (const VariableDependency* variable : live._variables) {
        if (usedAfterCall.contains(variable->getName())) {
            usedColumns._columns.push_back(live._columns[columnIndex]);
            usedColumns._variables.push_back(variable);
        } else {
            _deadVariables.insert(variable);
        }

        columnIndex++;
    }

    for (const VariableDependency* variable : live._edgeTypeVariables) {
        if (usedAfterCall.contains(variable->getName())) {
            usedColumns._columns.push_back(live._columns[columnIndex]);
            usedColumns._edgeTypeVariables.push_back(variable);
        } else {
            _deadVariables.insert(variable);
        }

        columnIndex++;
    }

    for (const size_t yieldedIndex : live._yieldedIndices) {
        if (usedAfterCall.contains(_yieldedColumns[yieldedIndex].first)) {
            usedColumns._columns.push_back(live._columns[columnIndex]);
            usedColumns._yieldedIndices.push_back(yieldedIndex);
        } else {
            _deadYieldedColumns.insert(yieldedIndex);
        }

        columnIndex++;
    }

    live = usedColumns;
}

void DBProgramGenerator::generateCall(const CallStmt* callStmt, const VariableNames& usedAfterCall) {
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
    llvm::SmallVector<std::string_view> yieldedVariables;
    if (yieldItems && !yieldItems->getItems().empty()) {
        for (const SymbolExpr* item : *yieldItems) {
            const Symbol* symbol = item->getSymbol();

            // The original name is the procedure's own name for the return value, which
            // is what the call op names; the symbol's name is what the query calls it,
            // which is the alias when the YIELD renamed it.
            yieldedNames.push_back(_opBuilder.getStringAttr(symbol->getOriginalName()));
            yieldedVariables.push_back(symbol->getName());
        }
    } else {
        // A call naming no return value produces every one the procedure declares, under
        // the procedure's own names - and none at all for a procedure declaring none,
        // which is called for what it does rather than for rows. Only a standalone call
        // gets here: the analyzer requires a YIELD of any call inside a query.
        for (const FunctionReturnType& returnType : signature->returnTypes()) {
            yieldedNames.push_back(_opBuilder.getStringAttr(returnType.getName()));
            yieldedVariables.push_back(returnType.getName());
        }
    }

    // One argument column per declared procedure argument, in the order written: a
    // variable resolves to its current column, a literal to a constant.
    llvm::SmallVector<mlir::Value> inputs;
    const ExprChain* arguments = invocation->getArguments();
    if (arguments) {
        for (const Expr* argument : *arguments) {
            translateExpr(argument);
            inputs.push_back(_exprMap.at(argument));
        }
    }

    // Everything already in flight rides through the carry set, so the projection can
    // still read it after the call: the procedure need not emit one row per row it was
    // given, and the call replicates a carried row once per row it emitted for it.
    LiveColumns live;
    collectLiveColumns(live);

    // A call reading none of those rows produces the same rows for every one of them,
    // which is their cartesian product rather than anything a carry set can express. The
    // choice reads the unfiltered set: rows in flight have to be paired with the call's
    // even when the projection returns none of their columns, or the query would lose
    // their cardinality.
    if (!live._columns.empty() && inputs.empty()) {
        generateCrossedCall(procedureName, yieldedNames, yieldedVariables, live);
        generateYieldFilter(yieldItems);
        return;
    }

    // Only what is still read rides through. Carrying a dead column would replicate it
    // once per emitted row for nothing, and - since only a procedure that reports its
    // input rows may be carried past at all - would refuse calls that never needed it.
    dropUnusedLiveColumns(usedAfterCall, live);

    // The yielded columns' value types come from the procedure's declared return types,
    // resolved during lowering, so they are left unresolved here - as a property fetch's
    // value column is. A carried column keeps its own type: the call replicates its rows,
    // it never retypes them.
    llvm::SmallVector<mlir::Type> resultTypes;
    for (size_t yieldIndex = 0; yieldIndex < yieldedNames.size(); yieldIndex++) {
        resultTypes.push_back(mlir::db::ColumnType::get(_mlirCtxt));
    }

    for (const mlir::Value column : live._columns) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto callOp = _opBuilder.create<mlir::db::CallProcedure>(loc,
                                                            resultTypes,
                                                            _opBuilder.getStringAttr(procedureName),
                                                            mlir::ValueRange {inputs},
                                                            mlir::ValueRange {live._columns},
                                                            _opBuilder.getArrayAttr(yieldedNames));

    // The yielded columns are new variables of the query, known by the name the YIELD
    // gave them; the carried ones are the rows already in flight, now aligned with what
    // the procedure emitted.
    const mlir::Operation::result_range results = callOp.getResults();
    for (size_t yieldIndex = 0; yieldIndex < yieldedVariables.size(); yieldIndex++) {
        _yieldedColumns.emplace_back(yieldedVariables[yieldIndex], results[yieldIndex]);
    }

    rebindLiveColumns(results, yieldedVariables.size(), live);

    generateYieldFilter(yieldItems);
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

    const auto findIt = _exprMap.find(predicateExpr);
    bioassert(findIt != end(_exprMap), "Failed to get value for YIELD WHERE expr");

    filterAllColumns(findIt->second);
}

void DBProgramGenerator::generateCrossedCall(std::string_view procedureName,
                                             llvm::ArrayRef<mlir::Attribute> yieldedNames,
                                             llvm::ArrayRef<std::string_view> yieldedVariables,
                                             const LiveColumns& live) {
    // The call reads none of the rows already in flight, so it produces the same rows for
    // every one of them: their cartesian product. Each side becomes a factor of a
    // db.cross_product - what the query has matched so far on the left, the call on the
    // right - and the product pairs them, which is exactly what it exists for.
    mlir::Block* const currentBlock = _opBuilder.getInsertionBlock();

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : live._columns) {
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

    // Everything generated so far becomes the left factor: it is a self-contained
    // dataflow already, so it moves wholesale - every op of this block ahead of the
    // product - and its db.yield names the columns it contributes.
    mlir::Block::OpListType& blockOps = currentBlock->getOperations();
    leftBlock->getOperations().splice(leftBlock->end(),
                                     blockOps,
                                     blockOps.begin(),
                                     crossProduct->getIterator());

    _opBuilder.setInsertionPointToEnd(leftBlock);
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {live._columns});

    // The call is the right factor, opening its own dataflow there: it takes no argument
    // and carries nothing, since the rows it is paired with are the other factor's.
    llvm::SmallVector<mlir::Type> callResultTypes;
    for (size_t yieldIndex = 0; yieldIndex < yieldedNames.size(); yieldIndex++) {
        callResultTypes.push_back(mlir::db::ColumnType::get(_mlirCtxt));
    }

    _opBuilder.setInsertionPointToEnd(rightBlock);
    auto callOp = _opBuilder.create<mlir::db::CallProcedure>(loc,
                                                            callResultTypes,
                                                            _opBuilder.getStringAttr(procedureName),
                                                            mlir::ValueRange {},
                                                            mlir::ValueRange {},
                                                            _opBuilder.getArrayAttr(yieldedNames));
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {callOp.getResults()});

    // Anything generated from here on reads the product's results, not the factors'.
    _opBuilder.setInsertionPointToEnd(currentBlock);

    // Its results are the left factor's columns then the right's: the rows already in
    // flight, now paired, followed by the call's own.
    const mlir::Operation::result_range results = crossProduct.getResults();
    rebindLiveColumns(results, /*firstResult=*/0, live);

    for (size_t yieldIndex = 0; yieldIndex < yieldedVariables.size(); yieldIndex++) {
        _yieldedColumns.emplace_back(yieldedVariables[yieldIndex],
                                     results[live._columns.size() + yieldIndex]);
    }
}

void DBProgramGenerator::generateCreate(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();
    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const StmtContainer* updateStmts = sglPart->getUpdateStmts();
    if (!updateStmts) {
        return;
    }

    // Collect MATCH-bound columns by variable name so CREATE patterns can reference them.
    std::unordered_map<std::string_view, mlir::Value> knownVars;
    for (const auto& [var, identities] : _varMap) {
        if (!identities.empty()) {
            knownVars[var->getName()] = identities.back();
        }
    }

    mlir::Value matchCardinality;
    if (!knownVars.empty()) {
        matchCardinality = knownVars.begin()->second;
    }

    const mlir::db::ColumnType nodeIDType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType edgeIDType = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    // Emit db.create_node for a node that is not already in knownVars, then register
    // the result under the node's variable name so later patterns can reference it.
    const auto resolveOrCreateNode = [&](const NodePattern* node) -> mlir::Value {
        const VarDecl* decl = node->getDecl();
        const std::string_view name = decl ? decl->getName() : "";

        if (!name.empty() && knownVars.contains(name)) {
            return knownVars.at(name);
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
                propValues.push_back(_exprMap.at(constraint._expr));
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

        if (!name.empty()) {
            knownVars[name] = nodeValue;
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
                        propValues.push_back(_exprMap.at(constraint._expr));
                    }
                }

                bioassert(edgeData && !edgeData->edgeTypeConstraints().empty(),
                          "CREATE edge must have an edge type");
                const std::string_view edgeType = edgeData->edgeTypeConstraints().front();

                const mlir::Value srcValue =
                    edge->getDirection() == EdgePattern::Direction::Backward ? rhsValue : lhsValue;
                const mlir::Value tgtValue =
                    edge->getDirection() == EdgePattern::Direction::Backward ? lhsValue : rhsValue;

                _opBuilder.create<mlir::db::CreateEdge>(
                    loc,
                    edgeIDType,
                    srcValue,
                    tgtValue,
                    _opBuilder.getStringAttr(edgeType),
                    _opBuilder.getStrArrayAttr(propNames),
                    mlir::ValueRange{propValues});

                lhsValue = rhsValue;
            }
        }
    }
}

mlir::Value DBProgramGenerator::resolveEntityColumn(std::string_view varName) {
    for (const auto& [var, values] : _varMap) {
        if (var->getName() == varName && !values.empty()) {
            return values.back();
        }
    }

    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();
    const auto findIt = edgeIdentities.find(std::string(varName));
    const bool foundEdgeIdentity = findIt != edgeIdentities.end() && !findIt->second.empty();
    if (foundEdgeIdentity) {
        const VariableDependency* representative = findIt->second.front();
        return _varMap.at(representative).back();
    }

    return mlir::Value {};
}

bool DBProgramGenerator::isRowAlignedHere(mlir::Value column) const {
    mlir::Operation* const definingOp = column.getDefiningOp();
    mlir::Block* const definingBlock = definingOp
        ? definingOp->getBlock()
        : mlir::cast<mlir::BlockArgument>(column).getOwner();

    return definingBlock == _opBuilder.getInsertionBlock();
}

mlir::Value DBProgramGenerator::resolveWildcardColumn() const {
    for (const VariableDependency& var : _vdg.vars()) {
        const auto findIt = _varMap.find(&var);
        if (findIt == _varMap.end() || findIt->second.empty()) {
            continue;
        }

        const mlir::Value column = findIt->second.back();
        if (!isRowAlignedHere(column)) {
            continue;
        }

        return column;
    }

    return mlir::Value {};
}

void DBProgramGenerator::generateSet(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();
    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const StmtContainer* updateStmts = sglPart->getUpdateStmts();
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

            const mlir::Value entityColumn = resolveEntityColumn(varName);
            bioassert(entityColumn, "SET on unknown variable: {}", varName);

            translateExpr(assign->_propValueExpr);
            const mlir::Value valueColumn = _exprMap.at(assign->_propValueExpr);

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

void DBProgramGenerator::generateDelete(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();
    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const StmtContainer* updateStmts = sglPart->getUpdateStmts();
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

            const mlir::Value entityColumn = resolveEntityColumn(varName);
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

void DBProgramGenerator::generateOutput(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    };

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const ReturnStmt* rtn = sglPart->getReturnStmt();
    if (!rtn) {
        // A standalone CALL has no projection of its own: what it emits is the columns it
        // yielded, in the order the procedure declares them. A call yielding none - a
        // procedure declaring no return value - emits nothing at all, so the query is the
        // drive and no output op is generated.
        llvm::SmallVector<mlir::Value> yielded;
        for (const YieldedColumn& yieldedColumn : _yieldedColumns) {
            yielded.push_back(yieldedColumn.second);
        }

        if (!yielded.empty()) {
            _opBuilder.create<mlir::db::Output>(_opBuilder.getUnknownLoc(), mlir::ValueRange {yielded});
        }

        return;
    }

    const Projection* proj = rtn->getProjection();

    const Projection::Items& returned = proj->items();

    VariableColumnMap variableColumns;
    for (auto& [cypherVar, mlirCol] : _varMap) {
        const std::string_view varName = cypherVar->getName();

        bioassert(not mlirCol.empty(), "No definitions for {}", varName);
        const mlir::Value finalValue = mlirCol.back();

        variableColumns[varName] = finalValue;
    }

    // A CALL's yielded columns are variables of the query too, named by the YIELD.
    for (const YieldedColumn& yieldedColumn : _yieldedColumns) {
        variableColumns[yieldedColumn.first] = yieldedColumn.second;
    }

    for (const auto& [name, vars] : _vdg.edgeIdentities()) {
        bioassert(!vars.empty(), "Empty edge identity for '{}'", name);
        const VariableDependency* representative = vars.front();
        bioassert(_varMap.contains(representative), "Edge identity representative not in varMap");
        variableColumns[name] = _varMap.at(representative).back();
    }

    const auto getVarForItem = [&](auto&& item) -> mlir::Value {
        using Type = std::remove_cvref_t<decltype(item)>;

        if constexpr (std::is_same_v<Type, VarDecl*>) {
            const std::string_view name = item->getName();
            const auto findIt = variableColumns.find(name);
            bioassert(findIt != end(variableColumns), "Return variable '{}' not found", name);
            return findIt->second;
        } else {
            return getOrTranslateExprColumn(variableColumns, item);
        }
    };

    // The analyzer names every item it accepts, so an item without one is left for the
    // sink to label by position rather than being a codegen error.
    const auto getNameForItem = [&](auto&& item) -> llvm::StringRef {
        const std::optional<std::string_view> name = proj->getName(item);
        if (!name) {
            return llvm::StringRef();
        }

        return llvm::StringRef(name->data(), name->size());
    };

    llvm::SmallVector<mlir::Value> outputted;
    llvm::SmallVector<llvm::StringRef> outputNames;
    for (const Projection::ReturnItem item : returned) {
        const mlir::Value itemCol = std::visit(getVarForItem, item);
        outputted.push_back(itemCol);
        outputNames.push_back(std::visit(getNameForItem, item));

        // An alias is one variable declared once, so a key naming it holds the very
        // declaration of the item it names: publishing the column under that declaration
        // is what lets the key read this column instead of computing a second one
        Expr* const* itemExpr = std::get_if<Expr*>(&item);
        if (!itemExpr) {
            continue;
        }

        const VarDecl* itemDecl = (*itemExpr)->getExprVarDecl();
        if (itemDecl) {
            _projectedColumns[itemDecl] = itemCol;
        }
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    // DISTINCT dedups the projection, and everything after it works on the rows that
    // survive: the sort orders the distinct rows, and SKIP and LIMIT cut them
    if (proj->isDistinct()) {
        translateDistinct(proj, outputted);
    }

    // ORDER BY reorders the whole projection, so it comes before them too: SKIP and
    // LIMIT cut the sorted rows
    if (proj->hasOrderBy()) {
        translateOrderBy(proj, variableColumns, outputted);
    }

    if (proj->hasSkip()) {
        const Expr* skipExpr = proj->getSkip()->getExpr();
        const int64_t skipValue = evaluateConstantInteger(skipExpr);

        if (skipValue < 0) {
            throw TuringException("SKIP expression must be a non-negative integer");
        }

        const uint64_t skipCount = static_cast<uint64_t>(skipValue);

        llvm::SmallVector<size_t> skippedItems;
        llvm::SmallVector<mlir::Value> skipped;
        collectCutColumns(proj, outputted, skippedItems, skipped);

        llvm::SmallVector<mlir::Type> skipResultTypes;
        for (const mlir::Value column : skipped) {
            skipResultTypes.push_back(column.getType());
        }

        auto skipOp = _opBuilder.create<mlir::db::Skip>(loc, skipResultTypes, mlir::ValueRange{skipped}, skipCount);

        const mlir::ResultRange skipResults = skipOp.getResults();
        for (size_t resultIndex = 0; resultIndex < skippedItems.size(); resultIndex++) {
            outputted[skippedItems[resultIndex]] = skipResults[resultIndex];
        }
    }

    if (proj->hasLimit()) {
        const Expr* limitExpr = proj->getLimit()->getExpr();
        const int64_t limitValue = evaluateConstantInteger(limitExpr);

        if (limitValue < 0) {
            throw TuringException("LIMIT expression must be a non-negative integer");
        }

        const uint64_t limitCount = static_cast<uint64_t>(limitValue);

        llvm::SmallVector<size_t> limitedItems;
        llvm::SmallVector<mlir::Value> limited;
        collectCutColumns(proj, outputted, limitedItems, limited);

        llvm::SmallVector<mlir::Type> limitResultTypes;
        for (const mlir::Value column : limited) {
            limitResultTypes.push_back(column.getType());
        }

        auto limitOp = _opBuilder.create<mlir::db::Limit>(loc, limitResultTypes, mlir::ValueRange{limited}, limitCount);

        const mlir::ResultRange limitResults = limitOp.getResults();
        for (size_t resultIndex = 0; resultIndex < limitedItems.size(); resultIndex++) {
            outputted[limitedItems[resultIndex]] = limitResults[resultIndex];
        }
    }

    _opBuilder.create<mlir::db::Output>(loc,
                                        mlir::ValueRange{outputted},
                                        _opBuilder.getStrArrayAttr(outputNames));
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
        const bool isConstantKey = !keyExpr->isDynamic();
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
    // One column is held per variable, under the name of that variable, so an expression
    // only has a column to be found there when it is a variable and nothing else.
    // Anything more is a computation over columns, and has to be translated
    if (expr->getKind() == Expr::Kind::SYMBOL) {
        const VarDecl* var = expr->getExprVarDecl();
        bioassert(var, "Symbol expression without a declaration.");

        const std::string_view name = var->getName();
        const auto findIt = variableColumns.find(name);
        bioassert(findIt != end(variableColumns), "No column for variable '{}'", name);

        return findIt->second;
    }

    translateExpr(expr);
    return _exprMap.at(expr);
}

void DBProgramGenerator::applyPredicateFilters(std::span<const Expr* const> predicates) {
    for (const Expr* predicate : predicates) {
        translateExpr(predicate);
        filterAllColumns(_exprMap.at(predicate));
    }
}

void DBProgramGenerator::generatePropertyConstraints(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const StmtContainer* stmtsContainer = sglPart->getReadStmts();
    if (!stmtsContainer) {
        return;
    }

    std::vector<const Expr*> constraintExprs;

    for (const Stmt* stmt : stmtsContainer->stmts()) {
        if (stmt->getKind() != Stmt::Kind::MATCH) {
            continue;
        }

        const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
        const Pattern* pattern = matchStmt->getPattern();

        constraintExprs.clear();

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
}

void DBProgramGenerator::applyConstraints(const VariableDependency* var) {
    const std::optional<VariableDependency::Constraint>& constraints = var->constraints();
    if (!constraints.has_value()) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const auto applyLabelConstraint = [&](const VariableDependency::LabelNames& labels) {
        const auto findIt = _varMap.find(var);
        const bool registered = findIt != _varMap.end() && !findIt->second.empty();
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
        const auto findIt = _edgeTypeMap.find(var);
        bioassert(findIt != _edgeTypeMap.end(),
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

void DBProgramGenerator::generateFilters(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const StmtContainer* stmtsContainer = sglPart->getReadStmts();
    if (!stmtsContainer) {
        return;
    }

    std::vector<const Expr*> conjuncts;
    for (const Stmt* stmt : stmtsContainer->stmts()) {
        if (stmt->getKind() != Stmt::Kind::MATCH) {
            continue;
        }

        const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
        const Pattern* pattern = matchStmt->getPattern();
        const WhereClause* where = pattern->getWhere();
        if (!where) {
            continue;
        }

        conjuncts.clear();
        flattenConjuncts(where->getExpr(), conjuncts);

        applyPredicateFilters(conjuncts);
    }
}

void DBProgramGenerator::translateExpr(const Expr* expr) {
    if (_exprMap.contains(expr)) {
        return;
    }

    const Expr::Kind kind = expr->getKind();
    switch (kind) {
        case Expr::Kind::PROPERTY: {
            const PropertyExpr* propExpr = static_cast<const PropertyExpr*>(expr);
            _exprMap[expr] = translatePropertyExpr(propExpr);
        }
        break;

        case Expr::Kind::LITERAL: {
            const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(expr);
            _exprMap[expr] = translateLiteralExpr(litExpr->getLiteral());
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
            const auto projectedIt = _projectedColumns.find(decl);

            if (projectedIt != end(_projectedColumns)) {
                _exprMap[expr] = projectedIt->second;
            } else {
                const mlir::Value entityColumn = resolveEntityColumn(varName);
                if (entityColumn) {
                    _exprMap[expr] = entityColumn;
                }
            }

            // A variable a CALL yielded appears in no pattern, so it is no VDG variable;
            // its column is the one the call produced for that return value.
            if (!_exprMap.contains(expr)) {
                if (const mlir::Value yielded = findYieldedColumn(varName)) {
                    _exprMap[expr] = yielded;
                }
            }

            bioassert(_exprMap.contains(expr), "Symbol refers to unknown variable: {}", varName);
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unaryExpr = static_cast<const UnaryExpr*>(expr);
            translateUnaryExpr(expr, unaryExpr);
        }
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(expr);
            translateFunctionInvocationExpr(expr, funcExpr);
        }
        break;

        case Expr::Kind::INDEX:
        case Expr::Kind::LIST:
        case Expr::Kind::STRING:
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
    bioassert(_exprMap.contains(subExpr), "Unary operation with unknown operand.");
    const mlir::Value operandVal = _exprMap.at(subExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (op) {
        case UnaryOperator::Not: {
            const mlir::db::ColumnType boolType =
                allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
            auto notOp = _opBuilder.create<mlir::db::NotOp>(loc, boolType, operandVal);
            const mlir::Value result = notOp.getResult();
            _exprMap[expr] = result;
        }
        break;

        case UnaryOperator::Minus: {
            // -x is 0 - x, the subtraction the engine already lowers: the promotion of the
            // zero against the operand is what gives the negation its type
            const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
            const mlir::TypedAttr zeroAttr = _opBuilder.getI64IntegerAttr(0);
            const mlir::db::ColumnType zeroType = allocColumnType(zeroAttr.getType());
            const mlir::Value zero = _opBuilder.create<mlir::db::ConstantOp>(loc, zeroType, zeroAttr).getResult();

            _exprMap[expr] = _opBuilder.create<mlir::db::SubOp>(loc, noneType, zero, operandVal).getResult();
        }
        break;

        case UnaryOperator::Plus:
            _exprMap[expr] = operandVal;
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

    bioassert(_exprMap.contains(lhsExpr), "Binary operation with unknown LHS operand.");
    bioassert(_exprMap.contains(rhsExpr), "Binary operation with unknown RHS operand.");

    const mlir::Value lhs = _exprMap.at(lhsExpr);
    const mlir::Value rhs = _exprMap.at(rhsExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    const BinaryOperator op = binExpr->getOperator();

    switch (op) {
        case BinaryOperator::Equal:
            _exprMap[expr] = _opBuilder.create<mlir::db::EqOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::And:
            _exprMap[expr] = _opBuilder.create<mlir::db::AndOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Or:
            _exprMap[expr] = _opBuilder.create<mlir::db::OrOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Add:
            _exprMap[expr] = _opBuilder.create<mlir::db::AddOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Sub:
            _exprMap[expr] = _opBuilder.create<mlir::db::SubOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Mult:
            _exprMap[expr] = _opBuilder.create<mlir::db::MulOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Div:
            _exprMap[expr] = _opBuilder.create<mlir::db::DivOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::GreaterThan:
            _exprMap[expr] = _opBuilder.create<mlir::db::GtOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::LessThan:
            _exprMap[expr] = _opBuilder.create<mlir::db::LtOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::GreaterThanOrEqual:
            _exprMap[expr] = _opBuilder.create<mlir::db::GteOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::LessThanOrEqual:
            _exprMap[expr] = _opBuilder.create<mlir::db::LteOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::NotEqual:
            _exprMap[expr] = _opBuilder.create<mlir::db::NeqOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Xor:
            _exprMap[expr] = _opBuilder.create<mlir::db::XorOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Mod:
            _exprMap[expr] = _opBuilder.create<mlir::db::ModOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Pow:
            _exprMap[expr] = _opBuilder.create<mlir::db::PowOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::In:
            throw TuringException(fmt::format("Unsupported operation: {}",
                                              BinaryOperatorDescription::value(op)));
        break;

        case BinaryOperator::_SIZE:
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
            const EmbeddingLiteral* embeddingLiteral = static_cast<const EmbeddingLiteral*>(literal);
            const std::span<const float> floats = embeddingLiteral->getValue();
            const mlir::FloatType f32Type = _opBuilder.getF32Type();
            const size_t size = floats.size();

            const mlir::RankedTensorType tensorType = mlir::RankedTensorType::get(size, f32Type);

            const auto* bytes = reinterpret_cast<const char*>(floats.data());
            const size_t numBytes = size * sizeof(float);

            const llvm::ArrayRef<char> rawBytes {bytes, numBytes};

            const mlir::TypedAttr embAttr = mlir::DenseElementsAttr::getFromRawBuffer(tensorType, rawBytes);
            const mlir::db::ColumnType embResultType = allocColumnType(mlir::storage::EmbeddingType::get(_mlirCtxt));
            return _opBuilder.create<mlir::db::ConstantOp>(uloc, embResultType, embAttr).getResult();
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

mlir::Value DBProgramGenerator::translatePropertyExpr(const PropertyExpr* propExpr) {
    const VarDecl* entityDecl = propExpr->getEntityVarDecl();
    const std::string_view varName = entityDecl->getName();
    const std::string_view propName = propExpr->getPropName();

    const mlir::Value entityColumn = resolveEntityColumn(varName);

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
    mlir::Value inputColumn;

    if (argExpr->getType() == EvaluatedType::Wildcard) {
        inputColumn = resolveWildcardColumn();
        bioassert(inputColumn, "count(*) over no column holding the rows it counts.");
    } else {
        translateExpr(argExpr);
        inputColumn = _exprMap.at(argExpr);
    }

    const bool isDistinct = invocation->isDistinct();

    // Dropping repeated values cannot move an extremum, so min(DISTINCT x) is min(x):
    // the flag is spent here rather than on a seen-set the result cannot depend on
    const bool isExtremum = funcName == "min" || funcName == "max";
    const bool reducesDistinctValues = isDistinct && !isExtremum;

    if (funcName == "count") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Count>(loc, noneType, inputColumn, isDistinct).getResult();
    } else if (funcName == "sum") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Sum>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "min") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Min>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "max") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Max>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "avg") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Avg>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "collect") {
        // collect gathers values rather than reducing them, so it is db.collect rather
        // than one of the reductions: with no grouping key it is the keyless form, whose
        // one row holds the whole match's values as a list
        const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, mlir::NoneType::get(_mlirCtxt));
        const mlir::db::ColumnType listColumnType = allocColumnType(listType);

        auto collectOp = _opBuilder.create<mlir::db::Collect>(loc,
                                                             mlir::TypeRange {listColumnType},
                                                             mlir::ValueRange {inputColumn},
                                                             /*keyCount=*/0,
                                                             reducesDistinctValues);
        _exprMap[expr] = collectOp.getResults().front();
    } else {
        throw TuringException(fmt::format("Unsupported aggregate function: {}", funcName));
    }
}

mlir::Value DBProgramGenerator::translateArg(const Expr* argExpr) {
    translateExpr(argExpr);
    bioassert(_exprMap.contains(argExpr), "Function invocation with unknown argument.");
    return _exprMap.at(argExpr);
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
        _exprMap[expr] = unaryIt->second(_opBuilder, loc, noneType, input);
        return;
    }

    const auto binaryIt = binaryFunctionEmitters.find(funcName);
    if (binaryIt != end(binaryFunctionEmitters)) {
        if (!args || args->size() != 2) {
            throw TuringException(fmt::format("{}() expects 2 arguments.", funcName));
        }

        const mlir::Value lhs = translateArg(args->getExprs()[0]);
        const mlir::Value rhs = translateArg(args->getExprs()[1]);
        _exprMap[expr] = binaryIt->second(_opBuilder, loc, noneType, lhs, rhs);
        return;
    }

    throw TuringException(fmt::format("Unsupported function: {}", funcName));
}

void DBProgramGenerator::generateGroupAggregate(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        return;
    }

    const QueryCommand* query = queries.front();
    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const ReturnStmt* rtn = sglPart->getReturnStmt();
    if (!rtn) {
        return;
    }

    const Projection* proj = rtn->getProjection();
    if (!proj->isAggregate() || !proj->hasGroupingKeys()) {
        return;
    }

    VariableColumnMap variableColumns;
    for (const auto& [cypherVar, mlirCol] : _varMap) {
        variableColumns[cypherVar->getName()] = mlirCol.back();
    }

    // An edge identity is carried by the traversal variable that produced it, under a
    // name of its own: the representative is where its column - and its grouped
    // replacement - is published, since the identity has no variable of its own
    llvm::StringMap<const VariableDependency*> edgeIdentityVars;
    for (const auto& [name, vars] : _vdg.edgeIdentities()) {
        if (!vars.empty() && _varMap.contains(vars.front())) {
            variableColumns[name] = _varMap.at(vars.front()).back();
            edgeIdentityVars[name] = vars.front();
        }
    }

    // Parallel: for each key position, exactly one of these is non-null
    llvm::SmallVector<mlir::Value> keyColumns;
    llvm::SmallVector<const VariableDependency*> keyVarAtPos;
    llvm::SmallVector<const Expr*> keyExprAtPos;

    llvm::SmallVector<mlir::Value> aggInputColumns;
    llvm::SmallVector<mlir::storage::GroupAggregateKind> aggKinds;
    llvm::SmallVector<const FunctionInvocationExpr*> aggFuncExprs;
    llvm::SmallVector<const FunctionInvocationExpr*> itemInvocations;

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (const Projection::ReturnItem& returnItem : proj->items()) {
        if (const VarDecl* const* varDeclPtr = std::get_if<VarDecl*>(&returnItem)) {
            const std::string_view name = (*varDeclPtr)->getName();
            const auto findIt = variableColumns.find(name);
            bioassert(findIt != variableColumns.end(), "Grouping key variable {} not found.", name);
            keyColumns.push_back(findIt->second);

            // An edge identity's column is published under its representative, and that
            // is the one the projection reads back, so the grouped column has to replace
            // it there rather than under a traversal variable of the same name
            const VariableDependency* keyVar = nullptr;
            const auto identityIt = edgeIdentityVars.find(name);

            if (identityIt != edgeIdentityVars.end()) {
                keyVar = identityIt->second;
            } else {
                for (auto& [var, values] : _varMap) {
                    if (var->getName() == name) {
                        keyVar = var;
                        break;
                    }
                }
            }

            bioassert(keyVar, "Grouping key variable {} has no column to group", name);
            keyVarAtPos.push_back(keyVar);
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
                _projectedColumns[itemDecl] = keyColumn;
            }

            // A constant column holds the same value in every row, so it tells no two
            // rows apart: it groups nothing and rides along beside the groups, as it
            // rides past a dedup or a sort
            if (yieldsConstantColumn(keyColumn)) {
                continue;
            }

            keyColumns.push_back(keyColumn);
            keyVarAtPos.push_back(nullptr);
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

    // A key may order the groups by an aggregate the projection does not return -
    // RETURN a.name ORDER BY count(b) - which the aggregation has to compute all the same.
    // Its result column is then read by the sort alone, and no db.output reads it, which is
    // what keeps it out of the rows
    if (proj->hasOrderBy()) {
        llvm::SmallVector<const FunctionInvocationExpr*> keyInvocations;
        for (const OrderByItem* orderByItem : proj->getOrderBy()->getItems()) {
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

        if (funcName == "collect") {
            // db.collect groups on its own rather than being one function of a
            // db.group_aggregate, so a collect beside a grouping key would need the two to
            // be grouped together - the keyless form is what the frontend generates today
            throw TuringException("collect() with grouping keys is not supported yet");
        }

        // Dropping repeated values cannot move an extremum, so min(DISTINCT x) is min(x)
        // and takes the plain kind. The others reduce each of a group's distinct values
        // once, which is a kind of its own, spelled as the function's name with the
        // modifier appended
        const bool isExtremum = funcName == "min" || funcName == "max";
        const bool reducesDistinctValues = invocation->isDistinct() && !isExtremum;

        const std::string kindName = reducesDistinctValues ? std::string(funcName) + "_distinct"
                                                           : std::string(funcName);

        const std::optional<mlir::storage::GroupAggregateKind> kind = mlir::storage::symbolizeGroupAggregateKind(kindName);
        if (!kind) {
            throw TuringException(fmt::format("Unsupported aggregate function: {}", funcName));
        }

        const ExprChain* args = invocation->getArguments();
        bioassert(args && !args->empty(), "Aggregate function invocation with no arguments.");

        const Expr* argExpr = args->front();
        mlir::Value inputColumn;

        if (argExpr->getType() == EvaluatedType::Wildcard) {
            inputColumn = resolveWildcardColumn();
            bioassert(inputColumn, "count(*) over no column holding the rows it counts.");
        } else {
            translateExpr(argExpr);
            inputColumn = _exprMap.at(argExpr);
        }

        aggInputColumns.push_back(inputColumn);
        aggKinds.push_back(*kind);
        aggFuncExprs.push_back(funcExpr);
    }

    const size_t keyCount = keyColumns.size();
    const size_t aggCount = aggInputColumns.size();
    bioassert(aggCount > 0, "grouped aggregate with no aggregate columns.");

    // Every non-aggregate item was constant, so nothing tells the matched rows apart:
    // the projection is one group, which is the scalar aggregate the projection
    // translates on its own - RETURN 1 AS x, count(n) counts the whole match
    if (keyCount == 0) {
        return;
    }

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

    auto groupAgg = _opBuilder.create<mlir::db::GroupAggregate>(
        loc,
        mlir::TypeRange{resultTypes},
        mlir::ValueRange{allColumns},
        static_cast<uint64_t>(keyCount),
        llvm::ArrayRef<int64_t>{aggKindValues});

    const mlir::ResultRange results = groupAgg.getResults();

    GroupedColumns groupedColumns;

    for (size_t i = 0; i < keyCount; i++) {
        if (keyVarAtPos[i]) {
            registerValue(keyVarAtPos[i], results[i]);
        } else {
            _exprMap[keyExprAtPos[i]] = results[i];
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
            const std::string_view symName = sym->getDecl()->getName();
            const auto identityIt = edgeIdentityVars.find(symName);

            if (identityIt != edgeIdentityVars.end()) {
                registerValue(identityIt->second, results[i]);
                continue;
            }

            for (auto& [var, values] : _varMap) {
                if (var->getName() == symName) {
                    registerValue(var, results[i]);
                    break;
                }
            }
        }
    }

    for (size_t i = 0; i < aggCount; i++) {
        const mlir::Value aggregateColumn = results[keyCount + i];
        _exprMap[aggFuncExprs[i]] = aggregateColumn;
        groupedColumns.emplace_back(aggFuncExprs[i], aggregateColumn);

        // The alias of an aggregate names its reduced column, so a key or a later item
        // spelling that alias reads this column instead of computing a second aggregate
        const VarDecl* aggregateDecl = aggFuncExprs[i]->getExprVarDecl();
        if (aggregateDecl) {
            _projectedColumns[aggregateDecl] = aggregateColumn;
        }
    }

    bindOrderByKeyColumns(proj, groupedColumns);
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
    if (!expr || _exprMap.contains(expr)) {
        return;
    }

    for (const auto& [keyExpr, keyColumn] : groupedColumns) {
        // The key reads this grouping key whole, so the grouped column is what it reads:
        // nothing below it has to be looked at, let alone translated again
        if (StructuralExpressionComparator::equal(keyExpr, expr)) {
            _exprMap[expr] = keyColumn;
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
