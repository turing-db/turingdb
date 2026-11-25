#include "DBProgramGenerator.h"

#include <algorithm>
#include <span>
#include <string_view>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"
#include "IRValueTypes.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "Symbol.h"
#include "WhereClause.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "stmt/CallStmt.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/ParameterExpr.h"
#include "expr/SymbolExpr.h"
#include "stmt/LoadCSVStmt.h"
#include "stmt/UnwindStmt.h"
#include "stmt/VectorSearchStmt.h"

#include "BioAssert.h"

using namespace db;

namespace {

// The name a VECTOR SEARCH gives the metric column; anything else it yields is the
// neighbour ID column, the analyzer having rejected every other name.
constexpr std::string_view vectorSearchScoreYield = "score";

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

void DBProgramGenerator::generateCall(const CallStmt* callStmt) {
    const FunctionInvocationExpr* funcExpr = callStmt->getFunc();
    if (!funcExpr) {
        throwError("CALL statement has no procedure invocation.", callStmt);
    }

    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
    if (!invocation) {
        throwError("CALL statement has no function invocation.", callStmt);
    }

    const FunctionSignature* signature = invocation->getSignature();
    if (!signature) {
        throwError("CALL statement has an unresolved procedure name.", callStmt);
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
            translateExprOverRows(argument);
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

    const Expr* path = loadCSVStmt->getPath();

    mlir::StringAttr pathAttr;
    mlir::Value boundPath;
    if (path->getKind() == Expr::Kind::PARAMETER) {
        boundPath = translateParameterExpr(static_cast<const ParameterExpr*>(path));
    } else {
        const Literal* literal = static_cast<const LiteralExpr*>(path)->getLiteral();
        const std::string_view spelled = static_cast<const StringLiteral*>(literal)->getValue();
        pathAttr = _opBuilder.getStringAttr(llvm::StringRef {spelled.data(), spelled.size()});
    }

    const mlir::ArrayAttr fieldsAttr = _opBuilder.getArrayAttr(fieldAttrs);

    const mlir::db::ColumnType fieldType = allocColumnType(mlir::storage::StringType::get(_mlirCtxt));
    const llvm::SmallVector<mlir::Type> fieldTypes(fieldAttrs.size(), fieldType);
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    const auto emitLoad = [&]() -> mlir::db::LoadCSV {
        return _opBuilder.create<mlir::db::LoadCSV>(loc,
                                                    fieldTypes,
                                                    pathAttr,
                                                    boundPath,
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

    // A scope rebind tells its columns apart by name, so each field goes under its own
    // declaration's rather than under the row's, which every field shares
    const std::string_view alias = loadCSVStmt->getAliasDecl()->getName();

    for (size_t index = 0; index < fieldDecls.size(); index++) {
        const VarDecl* fieldDecl = fieldDecls[index];
        const std::string_view name = fieldDecl ? fieldDecl->getName() : alias;

        _part._yieldedColumns.push_back({fieldDecl, name, fields[index]});

        if (fieldDecl) {
            _csvFieldRows[fieldDecl] = alias;
        }
    }
}

void DBProgramGenerator::generateVectorSearch(const VectorSearchStmt* vectorSearchStmt) {
    const YieldClause* yield = vectorSearchStmt->getYield();
    const YieldItems* yieldItems = yield ? yield->getItems() : nullptr;
    if (!yieldItems || yieldItems->getItems().empty()) {
        throwError("VECTOR SEARCH statement names no yielded value.", vectorSearchStmt);
    }

    VectorSearchOperands operands;
    translateVectorSearchOperands(vectorSearchStmt, operands);

    llvm::SmallVector<mlir::Value, 3> rowValues;
    for (const mlir::Value bound : {operands._boundIndexName, operands._boundNeighbourCount, operands._boundQueryVector}) {
        const bool readsTheRow = bound && !::db::readsNoRow(bound) && !yieldsConstantColumn(bound, _constantColumns);
        if (readsTheRow) {
            rowValues.push_back(bound);
        }
    }

    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    if (!rowValues.empty()) {
        generatePerRowVectorSearch(yieldItems, operands, rowValues, inFlight);
    } else if (!inFlight._columns.empty()) {
        generateCrossedVectorSearch(yieldItems, operands, inFlight);
    } else {
        mlir::db::VectorSearch search = createVectorSearch(operands);
        publishVectorSearchYields(yieldItems, search.getIds(), search.getScores());
    }

    generateYieldFilter(yieldItems);
}

void DBProgramGenerator::translateVectorSearchOperands(const VectorSearchStmt* vectorSearchStmt,
                                                       VectorSearchOperands& operands) {
    const auto literalOf = [](const Expr* expr, Literal::Kind kind) -> const Literal* {
        if (expr->getKind() != Expr::Kind::LITERAL) {
            return nullptr;
        }

        const Literal* literal = static_cast<const LiteralExpr*>(expr)->getLiteral();

        return literal->getKind() == kind ? literal : nullptr;
    };

    const auto translateOperand = [this](const Expr* expr) {
        translateExpr(expr);
        return _part._exprMap.at(expr);
    };

    const Expr* index = vectorSearchStmt->getIndex();
    if (const Literal* literal = literalOf(index, Literal::Kind::STRING)) {
        const std::string_view name = static_cast<const StringLiteral*>(literal)->getValue();
        operands._indexName = _opBuilder.getStringAttr(llvm::StringRef {name.data(), name.size()});
    } else {
        operands._boundIndexName = translateOperand(index);
    }

    const Expr* neighbourCount = vectorSearchStmt->getK();
    if (const Literal* literal = literalOf(neighbourCount, Literal::Kind::INTEGER)) {
        const int64_t spelled = static_cast<const IntegerLiteral*>(literal)->getValue();
        operands._neighbourCount = mlir::IntegerAttr::get(_opBuilder.getIntegerType(64, /*isSigned=*/false), spelled);
    } else {
        operands._boundNeighbourCount = translateOperand(neighbourCount);
    }

    const Expr* queryVector = vectorSearchStmt->getQueryVector();
    if (const Literal* literal = literalOf(queryVector, Literal::Kind::EMBEDDING)) {
        operands._queryVector = embeddingLiteralAttr(static_cast<const EmbeddingLiteral*>(literal));
    } else {
        operands._boundQueryVector = translateOperand(queryVector);
    }
}

mlir::db::VectorSearch DBProgramGenerator::createVectorSearch(const VectorSearchOperands& operands) {
    const mlir::db::ColumnType idType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType scoreType = allocColumnType(_opBuilder.getF64Type());

    return _opBuilder.create<mlir::db::VectorSearch>(_opBuilder.getUnknownLoc(),
                                                     idType,
                                                     scoreType,
                                                     operands._indexName,
                                                     operands._boundIndexName,
                                                     operands._neighbourCount,
                                                     operands._boundNeighbourCount,
                                                     operands._queryVector,
                                                     operands._boundQueryVector);
}

void DBProgramGenerator::generateCrossedVectorSearch(const YieldItems* yieldItems,
                                                     const VectorSearchOperands& operands,
                                                     const InFlightColumns& inFlight) {
    // The search reads none of the rows already in flight, so it produces the same neighbours
    // for every one of them: their cartesian product, which is what a call reading none of
    // them is paired with too.
    mlir::Block* const currentBlock = _opBuilder.getInsertionBlock();

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    resultTypes.push_back(allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt)));
    resultTypes.push_back(allocColumnType(_opBuilder.getF64Type()));

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    _opBuilder.setInsertionPointToEnd(currentBlock);
    mlir::db::CrossProduct crossProduct = _opBuilder.create<mlir::db::CrossProduct>(loc, resultTypes);

    mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();
    mlir::Block* const rightBlock = &crossProduct.getRightFactor().front();

    moveDataflowIntoLeftFactor(crossProduct);
    hoistConstantsOutOfLeftFactor(crossProduct);

    llvm::SmallVector<mlir::Value, 3> boundValues;
    for (const mlir::Value bound : {operands._boundIndexName, operands._boundNeighbourCount, operands._boundQueryVector}) {
        if (bound) {
            boundValues.push_back(bound);
        }
    }

    moveIntoRightFactor(crossProduct, boundValues);

    _opBuilder.setInsertionPointToEnd(leftBlock);
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {inFlight._columns});

    _opBuilder.setInsertionPointToEnd(rightBlock);
    mlir::db::VectorSearch search = createVectorSearch(operands);
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {search.getIds(), search.getScores()});

    // Anything generated from here on reads the product's results, not the factors'.
    _opBuilder.setInsertionPointToEnd(currentBlock);

    const mlir::Operation::result_range results = crossProduct.getResults();
    rebindInFlightColumns(results, /*firstResult=*/0, inFlight);

    const size_t searchOffset = inFlight._columns.size();
    publishVectorSearchYields(yieldItems, results[searchOffset], results[searchOffset + 1]);
}

void DBProgramGenerator::generatePerRowVectorSearch(const YieldItems* yieldItems,
                                                    const VectorSearchOperands& operands,
                                                    llvm::ArrayRef<mlir::Value> rowValues,
                                                    const InFlightColumns& inFlight) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const outerBlock = _opBuilder.getInsertionBlock();

    llvm::SmallVector<mlir::Value> inputColumns(inFlight._columns.begin(), inFlight._columns.end());
    for (const mlir::Value rowValue : rowValues) {
        if (!llvm::is_contained(inputColumns, rowValue)) {
            inputColumns.push_back(rowValue);
        }
    }

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : inputColumns) {
        resultTypes.push_back(column.getType());
    }

    resultTypes.push_back(allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt)));
    resultTypes.push_back(allocColumnType(_opBuilder.getF64Type()));

    mlir::db::CallSubquery callOp = _opBuilder.create<mlir::db::CallSubquery>(loc,
                                                                             resultTypes,
                                                                             inputColumns,
                                                                             /*unit=*/false,
                                                                             /*carriesScope=*/false,
                                                                             /*optional=*/false);

    mlir::Block* const bodyBlock = &callOp.getBody().emplaceBlock();
    for (const mlir::Value column : inputColumns) {
        bodyBlock->addArgument(column.getType(), loc);
    }

    _opBuilder.setInsertionPointToStart(bodyBlock);

    VectorSearchOperands bodyOperands = operands;
    for (mlir::Value* bound : {&bodyOperands._boundIndexName, &bodyOperands._boundNeighbourCount, &bodyOperands._boundQueryVector}) {
        if (!llvm::is_contained(rowValues, *bound)) {
            continue;
        }

        const size_t inputIndex = static_cast<size_t>(llvm::find(inputColumns, *bound) - inputColumns.begin());
        const mlir::BlockArgument argument = bodyBlock->getArgument(static_cast<unsigned>(inputIndex));
        *bound = _opBuilder.create<mlir::db::RowConstant>(loc, argument).getResult();
    }

    mlir::db::VectorSearch search = createVectorSearch(bodyOperands);
    _opBuilder.create<mlir::db::SubqueryYield>(loc, mlir::Value(), mlir::ValueRange {search.getIds(), search.getScores()});

    _opBuilder.setInsertionPointToEnd(outerBlock);

    const mlir::ResultRange results = callOp.getResults();
    rebindInFlightColumns(results, /*firstResult=*/0, inFlight);

    const size_t searchOffset = inputColumns.size();
    publishVectorSearchYields(yieldItems, results[searchOffset], results[searchOffset + 1]);
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

    const Expr* arg = unwind->arg();
    const mlir::Value source = getOrTranslateExprColumn(variableColumns, arg);

    // Everything already in flight rides through the carry set, replicated once per row
    // the cell beside it unwound into, so the rest of the query still reads it row-aligned
    // with the elements.
    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    // The element column takes the type the source's elements carry. A carried column keeps
    // its own type: the unwind replicates its rows, it never retypes them.
    const mlir::Type sourceElement = mlir::cast<mlir::db::ColumnType>(source.getType()).getType();
    llvm::SmallVector<mlir::Type> resultTypes {allocColumnType(unwoundElementType(_mlirCtxt, sourceElement))};
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
    translateExprOverRows(predicateExpr);

    const auto findIt = _part._exprMap.find(predicateExpr);
    bioassert(findIt != _part._exprMap.end(), "Failed to get value for YIELD WHERE expr");

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

void DBProgramGenerator::moveIntoRightFactor(mlir::db::CrossProduct crossProduct, mlir::ValueRange values) {
    mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();
    mlir::Block* const rightBlock = &crossProduct.getRightFactor().front();

    llvm::DenseSet<mlir::Operation*> definingOps;
    collectDefiningOps(values, definingOps);

    llvm::SmallVector<mlir::Operation*> definingOpsInOrder;
    for (mlir::Operation& op : *leftBlock) {
        if (definingOps.contains(&op)) {
            definingOpsInOrder.push_back(&op);
        }
    }

    for (mlir::Operation* op : definingOpsInOrder) {
        op->moveBefore(rightBlock, rightBlock->end());
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
    moveIntoRightFactor(crossProduct, inputs);
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
