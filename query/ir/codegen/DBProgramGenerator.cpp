#include "DBProgramGenerator.h"

#include <algorithm>
#include <numbers>
#include <span>
#include <string_view>

#include "EntityPattern.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPassPipeline.h"
#include "DBPasses.h"
#include "DBTypes.h"
#include "DBSystemProgramGenerator.h"
#include "DBBoundColumns.h"
#include "StorageAttributes.h"
#include "StorageDialect.h"
#include "StorageEnums.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"
#include "ExplainReport.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "UnionQuery.h"
#include "ConditionalQuery.h"
#include "stmt/CallStmt.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/StmtContainer.h"
#include "decl/EvaluatedType.h"
#include "decl/ParameterDecl.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/SymbolExpr.h"
#include "stmt/CreateStmt.h"
#include "stmt/MergeStmt.h"
#include "stmt/NextSegmentStmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/ShortestPathStmt.h"
#include "stmt/UnwindStmt.h"
#include "stmt/VectorSearchStmt.h"
#include "stmt/WithStmt.h"

#include "BioAssert.h"

using namespace db;

namespace {

mlir::storage::UnicodeNormalForm toStorageNormalForm(UnicodeNormalForm form) {
    switch (form) {
        case UnicodeNormalForm::NFC:
            return mlir::storage::UnicodeNormalForm::NFC;
        break;
        case UnicodeNormalForm::NFD:
            return mlir::storage::UnicodeNormalForm::NFD;
        break;
        case UnicodeNormalForm::NFKC:
            return mlir::storage::UnicodeNormalForm::NFKC;
        break;
        case UnicodeNormalForm::NFKD:
            return mlir::storage::UnicodeNormalForm::NFKD;
        break;
        default:
            bioassert(false, "Unhandled Unicode normal form");
            return mlir::storage::UnicodeNormalForm::NFC;
        break;
    }
}

using UnaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder, mlir::Location loc, mlir::Value input);

template <typename Op>
mlir::Value emitUnaryFunction(mlir::OpBuilder& builder, mlir::Location loc, mlir::Value input) {
    return builder.create<Op>(loc, input).getResult();
}

const std::unordered_map<std::string_view, UnaryFunctionEmitter> unaryFunctionEmitters = {
    {"labels", &emitUnaryFunction<mlir::db::Labels>},
    {"type", &emitUnaryFunction<mlir::db::EdgeType>},
    {"keys", &emitUnaryFunction<mlir::db::Keys>},
    {"properties", &emitUnaryFunction<mlir::db::PropertyMap>},
    {"startNode", &emitUnaryFunction<mlir::db::StartNode>},
    {"endNode", &emitUnaryFunction<mlir::db::EndNode>},
    {"toInteger", &emitUnaryFunction<mlir::db::ToInteger>},
    {"toFloat", &emitUnaryFunction<mlir::db::ToFloat>},
    {"toString", &emitUnaryFunction<mlir::db::ToString>},
    {"toBoolean", &emitUnaryFunction<mlir::db::ToBoolean>},
    {"toIntegerOrNull", &emitUnaryFunction<mlir::db::ToIntegerOrNull>},
    {"toFloatOrNull", &emitUnaryFunction<mlir::db::ToFloatOrNull>},
    {"toStringOrNull", &emitUnaryFunction<mlir::db::ToStringOrNull>},
    {"toBooleanOrNull", &emitUnaryFunction<mlir::db::ToBooleanOrNull>},
    {"toIntegerList", &emitUnaryFunction<mlir::db::ToIntegerList>},
    {"toFloatList", &emitUnaryFunction<mlir::db::ToFloatList>},
    {"toStringList", &emitUnaryFunction<mlir::db::ToStringList>},
    {"toBooleanList", &emitUnaryFunction<mlir::db::ToBooleanList>},
    {"datetime", &emitUnaryFunction<mlir::db::ToDateTime>},
    {"duration", &emitUnaryFunction<mlir::db::ToDuration>},
    {"point", &emitUnaryFunction<mlir::db::ToPoint>},
    {"size", &emitUnaryFunction<mlir::db::Size>},
    {"length", &emitUnaryFunction<mlir::db::Size>},
    {"isEmpty", &emitUnaryFunction<mlir::db::IsEmpty>},
    {"char_length", &emitUnaryFunction<mlir::db::CharLength>},
    {"character_length", &emitUnaryFunction<mlir::db::CharLength>},
    {"head", &emitUnaryFunction<mlir::db::Head>},
    {"last", &emitUnaryFunction<mlir::db::Last>},
    {"tail", &emitUnaryFunction<mlir::db::Tail>},
    {"abs", &emitUnaryFunction<mlir::db::Abs>},
    {"sign", &emitUnaryFunction<mlir::db::Sign>},
    {"ceil", &emitUnaryFunction<mlir::db::Ceil>},
    {"floor", &emitUnaryFunction<mlir::db::Floor>},
    {"round", &emitUnaryFunction<mlir::db::Round>},
    {"sqrt", &emitUnaryFunction<mlir::db::Sqrt>},
    {"exp", &emitUnaryFunction<mlir::db::Exp>},
    {"log", &emitUnaryFunction<mlir::db::Log>},
    {"log10", &emitUnaryFunction<mlir::db::Log10>},
    {"sin", &emitUnaryFunction<mlir::db::Sin>},
    {"cos", &emitUnaryFunction<mlir::db::Cos>},
    {"tan", &emitUnaryFunction<mlir::db::Tan>},
    {"cot", &emitUnaryFunction<mlir::db::Cot>},
    {"asin", &emitUnaryFunction<mlir::db::Asin>},
    {"acos", &emitUnaryFunction<mlir::db::Acos>},
    {"atan", &emitUnaryFunction<mlir::db::Atan>},
    {"degrees", &emitUnaryFunction<mlir::db::Degrees>},
    {"radians", &emitUnaryFunction<mlir::db::Radians>},
    {"haversin", &emitUnaryFunction<mlir::db::Haversin>},
    {"toUpper", &emitUnaryFunction<mlir::db::ToUpper>},
    {"toLower", &emitUnaryFunction<mlir::db::ToLower>},
    {"trim", &emitUnaryFunction<mlir::db::Trim>},
    {"ltrim", &emitUnaryFunction<mlir::db::LTrim>},
    {"rtrim", &emitUnaryFunction<mlir::db::RTrim>},
    {"reverse", &emitUnaryFunction<mlir::db::Reverse>},
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
    {"coll.indexOf", &emitBinaryFunction<mlir::db::IndexOf>},
};

}

DBProgramGenerator::DBProgramGenerator(mlir::ModuleOp* mainModule,
                                       ExplainReport* explain,
                                       const mlir::db::DBPassContext& passContext)
    : _module(mainModule),
    _mlirCtxt(_module->getContext()),
    _opBuilder(_module->getBodyRegion()),
    _explain(explain),
    _passContext(passContext)
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

void DBProgramGenerator::throwError(std::string_view msg, const void* obj) const {
    _ast->getDiagnosticsManager()->throwError(msg, obj);
}

void DBProgramGenerator::createMain(std::span<ParameterDecl* const> parameters) {
    bioassert(_module, "Null module");
    bioassert(_mlirCtxt, "Null context");

    _mlirCtxt->loadDialect<mlir::db::DB>();
    _mlirCtxt->loadDialect<mlir::storage::Storage>();
    _mlirCtxt->loadDialect<mlir::func::FuncDialect>();

    llvm::SmallVector<mlir::Type> argumentTypes;
    llvm::SmallVector<mlir::Attribute> argumentAttributes;
    for (const ParameterDecl* decl : parameters) {
        argumentTypes.push_back(allocColumnType(parameterType(decl)));

        const mlir::StringAttr name = _opBuilder.getStringAttr(decl->getName());
        argumentAttributes.push_back(_opBuilder.getDictionaryAttr(_opBuilder.getNamedAttr(mlir::storage::PARAMETER_NAME_ATTRIBUTE, name)));
    }

    _opBuilder.setInsertionPointToEnd(_module->getBody());

    const mlir::FunctionType funcType = mlir::FunctionType::get(_mlirCtxt, argumentTypes, {});
    auto func = _opBuilder.create<mlir::func::FuncOp>(_opBuilder.getUnknownLoc(), "main", funcType);
    func.setAllArgAttrs(argumentAttributes);
    mlir::Block& block = *func.addEntryBlock();

    _parameterArguments.clear();
    for (const auto [decl, argument] : llvm::zip_equal(parameters, block.getArguments())) {
        _parameterArguments[decl] = argument;
    }

    _opBuilder.setInsertionPointToStart(&block);
}

void DBProgramGenerator::generate(const CypherAST* ast) {
    _ast = ast;
    _constantColumns.clear();
    _vdg.setDiagnosticsManager(ast->getDiagnosticsManager());

    createMain(ast->parameterDecls());

    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    if (generateSystemCommand(ast)) {
        _opBuilder.create<mlir::func::ReturnOp>(uloc);

        explainModule(ExplainStage::CODEGEN);
        explainModule(ExplainStage::DB);
        return;
    }

    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throwError("Multiple queries not yet supported.", queries.front());
    }

    const QueryCommand* command = queries.front();

    const QueryCommand::Kind kind = command->getKind();

    if (kind == QueryCommand::Kind::UNION_QUERY) {
        generateUnion(static_cast<const UnionQuery*>(command));
    } else if (kind == QueryCommand::Kind::CONDITIONAL_QUERY) {
        generateBodyOutput(static_cast<const ConditionalQuery*>(command), nullptr);
    } else if (kind == QueryCommand::Kind::SINGLE_PART_QUERY) {
        generateQuery(static_cast<const SinglePartQuery*>(command), nullptr);
    } else {
        throwError("Non-single part queries are not yet supported.", command);
    }

    _opBuilder.create<mlir::func::ReturnOp>(uloc);

    explainModule(ExplainStage::CODEGEN);

    runPasses();

    explainModule(ExplainStage::DB);
}

void DBProgramGenerator::generateQuery(const SinglePartQuery* query, const UnionBranch* branch) {
    generateQueryParts(query);

    const ReturnStmt* returnStmt = query->getReturnStmt();
    const Projection* projection = returnStmt ? returnStmt->getProjection() : nullptr;

    if (projection) {
        generateGroupAggregate(projection);
        generateOutput(projection, branch);
    } else {
        generateYieldedOutput(query);
    }
}

void DBProgramGenerator::generateQueryParts(const SinglePartQuery* query) {
    const StmtContainer* queryStmts = query->getStmts();
    if (!queryStmts) {
        return;
    }

    const std::span<Stmt* const> stmts {queryStmts->stmts()};

    // A WITH closes a query part: the statements before it feed its projection, and the
    // columns that projection publishes are all the statements after it can read. A
    // statement casting a value opens a part too, so that a row the clauses before it
    // dropped never reaches the cast, which would raise on it
    llvm::SmallVector<const VarDecl*> casts;
    size_t partBegin = 0;
    for (size_t index = 0; index < stmts.size(); index++) {
        const Stmt* stmt = stmts[index];

        const size_t castCount = casts.size();
        collectCheckedCasts(stmts.subspan(index, 1), casts);

        const bool castsAValueFirst = casts.size() > castCount;
        if (castsAValueFirst && index > partBegin) {
            generatePartStatements(stmts.subspan(partBegin, index - partBegin));
            publishInFlightColumns();
            partBegin = index;
        }

        if (stmt->getKind() == Stmt::Kind::WITH) {
            generatePartStatements(stmts.subspan(partBegin, index - partBegin));
            generateWith(static_cast<const WithStmt*>(stmt));
            partBegin = index + 1;
        } else if (stmt->getKind() == Stmt::Kind::NEXT_SEGMENT) {
            generatePartStatements(stmts.subspan(partBegin, index - partBegin));
            generateNextSegment(static_cast<const NextSegmentStmt*>(stmt));
            partBegin = index + 1;
        } else if (closesPartOnItsCut(stmt, stmts.subspan(index + 1))) {
            generatePartStatements(stmts.subspan(partBegin, index + 1 - partBegin));
            publishInFlightColumns();
            partBegin = index + 1;
        }
    }

    generatePartStatements(stmts.subspan(partBegin));
}

// The reading clauses of a part build the rows its updating clauses write one entity per,
// so the whole traversal is generated before the first write. The analyzer has already
// rejected a part that reads again after writing, so the split is a single cut
void DBProgramGenerator::generatePartStatements(std::span<Stmt* const> stmts) {
    bindCheckedCasts(stmts);

    const auto isUpdating = [](const Stmt* stmt) {
        return Stmt::isUpdating(stmt);
    };

    const auto firstUpdate = std::ranges::find_if(stmts, isUpdating);
    const size_t readingCount = static_cast<size_t>(firstUpdate - stmts.begin());

    if (readingCount > 0) {
        generateOptionalParts(stmts.subspan(0, readingCount));
    }

    generateUpdates(stmts.subspan(readingCount));
}

void DBProgramGenerator::generateOptionalParts(std::span<Stmt* const> stmts) {
    // A CALL subquery closes the traversal before it as an optional pattern does: its body
    // reads the rows in flight through a region of its own
    const auto closesTheTraversal = [](const Stmt* stmt) {
        const Stmt::Kind kind = stmt->getKind();

        if (kind == Stmt::Kind::MATCH) {
            return static_cast<const MatchStmt*>(stmt)->isOptional();
        }

        return kind == Stmt::Kind::CALL_SUBQUERY;
    };

    size_t partBegin = 0;
    for (size_t index = 0; index < stmts.size(); index++) {
        const Stmt* stmt = stmts[index];
        if (!closesTheTraversal(stmt)) {
            continue;
        }

        generatePart(stmts.subspan(partBegin, index - partBegin));

        if (stmt->getKind() == Stmt::Kind::MATCH) {
            generateOptionalMatch(stmts.subspan(index, 1));
        } else {
            generateCallSubquery(static_cast<const CallSubqueryStmt*>(stmt));
        }

        partBegin = index + 1;
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

        if (kind == Stmt::Kind::WITH || kind == Stmt::Kind::NEXT_SEGMENT) {
            return false;
        } else if (kind == Stmt::Kind::MATCH || kind == Stmt::Kind::CALL_SUBQUERY) {
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

void DBProgramGenerator::collectCheckedCasts(std::span<Stmt* const> stmts, llvm::SmallVectorImpl<const VarDecl*>& casts) {
    const auto collect = [&casts](const VarDecl* decl) {
        const bool isACast = decl && decl->getCastSource();
        if (isACast && !llvm::is_contained(casts, decl)) {
            casts.push_back(decl);
        }
    };

    for (const Stmt* stmt : stmts) {
        const Stmt::Kind kind = stmt->getKind();
        const Pattern* pattern = nullptr;

        if (kind == Stmt::Kind::MATCH) {
            pattern = static_cast<const MatchStmt*>(stmt)->getPattern();
        } else if (kind == Stmt::Kind::CREATE) {
            pattern = static_cast<const CreateStmt*>(stmt)->getPattern();
        } else if (kind == Stmt::Kind::MERGE) {
            pattern = static_cast<const MergeStmt*>(stmt)->getPattern();
        } else if (kind == Stmt::Kind::SHORTESTPATH) {
            const ShortestPathStmt* shortestPath = static_cast<const ShortestPathStmt*>(stmt);
            collect(shortestPath->getSourceDecl());
            collect(shortestPath->getTargetDecl());
        }

        if (!pattern) {
            continue;
        }

        for (const PatternElement* element : pattern->elements()) {
            for (const EntityPattern* entity : element->getEntities()) {
                collect(entity->getDecl());
            }
        }
    }
}

void DBProgramGenerator::bindCheckedCasts(std::span<Stmt* const> stmts) {
    llvm::SmallVector<const VarDecl*> casts;
    collectCheckedCasts(stmts, casts);

    const VariableDependencyGraph::BoundVars& boundVars = _vdg.boundVars();

    for (const VarDecl* cast : casts) {
        const auto bindsTheCast = [cast](const VariableDependency* var) {
            return var->getDecl() == cast;
        };

        if (llvm::any_of(boundVars, bindsTheCast)) {
            continue;
        }

        const VarDecl* source = cast->getCastSource();
        const auto bindsTheSource = [source](const VariableDependency* var) {
            return var->getDecl() == source;
        };

        const auto boundSourceIt = llvm::find_if(boundVars, bindsTheSource);
        bioassert(boundSourceIt != boundVars.end(), "Value cast as '{}' not bound when its part opens", cast->getName());

        const mlir::Value column = checkedCastColumn(cast, findVarOrThrow(_part._varMap, *boundSourceIt));
        const VariableDependency* var = _vdg.registerBoundVariable(cast->getName(), cast);
        registerValue(var, column);
    }
}

mlir::Value DBProgramGenerator::checkedCastColumn(const VarDecl* cast, mlir::Value value) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    mlir::Value perRow = value;
    if (yieldsConstantColumn(value, _constantColumns)) {
        perRow = _opBuilder.create<mlir::db::BroadcastConstant>(loc, value, resolveRowCarryingColumn()).getResult();
    }

    const bool castsANode = cast->getType() == EvaluatedType::NodePattern;
    const mlir::Type entityType = castsANode ? mlir::Type(mlir::storage::NodeIDType::get(_mlirCtxt))
                                             : mlir::Type(mlir::storage::EdgeIDType::get(_mlirCtxt));

    return _opBuilder.create<mlir::db::CheckedCast>(loc, allocColumnType(entityType), perRow).getResult();
}

void DBProgramGenerator::generatePart(std::span<Stmt* const> stmts) {
    _vdg.build(stmts);
    collectQuantifiedEdges(stmts);

    explainDependencyGraph();

    generateLeadingYields(stmts);
    generateTraversal(stmts);
    throwOnUnboundPatternVariable();
    throwOnDroppedUnwindSeed();
    closeBoundMerges();
    closeBoundEdges();
    resolveEdgeIdentities();
    generateCSVLoads(stmts);
    generateStatementOperations(stmts);
    resolveYieldedIdentities();
    generateShortestPath(stmts);
}

bool DBProgramGenerator::generateSystemCommand(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throwError("Multiple queries not yet supported.", queries.front());
    }

    DBSystemProgramGenerator systemGenerator(&_opBuilder);

    return systemGenerator.generate(queries.front());
}

void DBProgramGenerator::runPasses() {
    if (explainsPasses()) {
        runExplainedPasses();
    } else if (_passPipeline) {
        _passPipeline->run(*_module, _passContext);
    } else {
        DBPassPipeline passPipeline(_mlirCtxt);
        passPipeline.run(*_module, _passContext);
    }
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
            generateNamedPaths(matchStmt);
            generateMatchConstraints(matchStmt);
            generateMatchFilter(matchStmt);

            if (!matchStmt->isOptional()) {
                generateMatchOrderBy(matchStmt);
                generateMatchWindow(matchStmt);
            }
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

void DBProgramGenerator::translateFunctionInvocationExpr(const Expr* expr,
                                                         const FunctionInvocationExpr* funcExpr) {
    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
    const FunctionSignature* signature = invocation->getSignature();

    // The expression's own aggregate flag is contaminated by its arguments, so a scalar
    // function over a reduction - coalesce(max(n.age), 0) - carries it too. What says a
    // call reduces its input is the signature it resolved to.
    if (!signature->isAggregate()) {
        translateFunctionExpr(expr, invocation);
        return;
    }

    AggregateArguments arguments;
    translateAggregateArguments(invocation, nullptr, arguments);

    emitAggregate(expr, invocation, arguments);
}

mlir::Value DBProgramGenerator::translateArg(const Expr* argExpr) {
    translateExpr(argExpr);
    bioassert(_part._exprMap.contains(argExpr), "Function invocation with unknown argument.");
    return _part._exprMap.at(argExpr);
}

// A MERGE's rows mix the entities it wrote with the ones it bound, so no one label set or
// type stands for its column and it records neither: the caller falls back on the fetch,
// which reads each row where its own entity lives
mlir::Value DBProgramGenerator::translateCreatedMetadata(std::string_view funcName, const Expr* argExpr) {
    if (argExpr->getKind() != Expr::Kind::SYMBOL) {
        return {};
    }

    const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(argExpr);
    const PartScope::WrittenEntity* written = findWrittenEntity(symbolExpr->getDecl());
    if (!written) {
        return {};
    }

    const bool canReadLabels = funcName == "labels" && !written->_labels.empty() && written->_labelsKnown;
    const bool canReadType = funcName == "type" && !written->_edgeType.empty() && written->_edgeTypeKnown;

    if (canReadLabels) {
        return constantLabelList(written->_labels);
    } else if (canReadType) {
        return constantString(written->_edgeType);
    } else {
        return {};
    }
}

void DBProgramGenerator::translateFunctionExpr(const Expr* expr,
                                               const FunctionInvocation* invocation) {
    const std::string_view funcName = invocation->getSignature()->getFullName();
    const ExprChain* args = invocation->getArguments();

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    if (expr->getType() == EvaluatedType::Null) {
        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    const bool isEntityMetadata = funcName == "labels" || funcName == "type";
    if (isEntityMetadata && args && args->size() == 1) {
        const mlir::Value createdMetadata = translateCreatedMetadata(funcName, args->front());
        if (createdMetadata) {
            _part._exprMap[expr] = createdMetadata;
            return;
        }
    }

    if (funcName == "coalesce") {
        translateCoalesce(expr, args);
        return;
    } else if (funcName == "range") {
        translateRange(expr, args);
        return;
    } else if (funcName == "substring") {
        translateSubstring(expr, args);
        return;
    } else if (funcName == "split") {
        translateSplit(expr, args);
        return;
    } else if (funcName == "left") {
        translateStringEnd<mlir::db::Left>(expr, args);
        return;
    } else if (funcName == "right") {
        translateStringEnd<mlir::db::Right>(expr, args);
        return;
    } else if (funcName == "replace") {
        translateReplace(expr, args);
        return;
    } else if (funcName == "coll.sortMulti") {
        translateSortMulti(expr, args);
        return;
    } else if (funcName == "point.distance") {
        translatePointDistance(expr, args);
        return;
    } else if (funcName == "point.withinBBox") {
        translatePointWithinBBox(expr, args);
        return;
    } else if (funcName == "normalize") {
        translateNormalFormFunction<mlir::db::Normalize>(expr, args);
        return;
    } else if (funcName == "is_normalized") {
        translateNormalFormFunction<mlir::db::IsNormalized>(expr, args);
        return;
    }

    const bool readsTheMapItself = funcName == "properties"
                                && args && args->size() == 1
                                && args->front()->getType() == EvaluatedType::Map;
    if (readsTheMapItself) {
        _part._exprMap[expr] = translateArg(args->front());
        return;
    }

    // length() of a tagged cell reads a string or a list as well, so it is a size; nodes() and
    // relationships() of one read a path or nothing
    const Expr* const pathExpr = args && args->size() == 1 ? args->front() : nullptr;
    const EvaluatedType pathType = pathExpr ? pathExpr->getType() : EvaluatedType::Invalid;
    const bool readsATaggedPath = pathType == EvaluatedType::Any && (funcName == "nodes" || funcName == "relationships");
    const bool readsANamedPath = pathType == EvaluatedType::GraphPath || readsATaggedPath;
    if (readsANamedPath) {
        const mlir::Value path = translateArg(pathExpr);

        if (funcName == "length") {
            _part._exprMap[expr] = pathElementsColumn(path, mlir::storage::PathElementsKind::Length);
        } else if (funcName == "nodes") {
            _part._exprMap[expr] = pathElementsColumn(path, mlir::storage::PathElementsKind::Nodes);
        } else if (funcName == "relationships") {
            _part._exprMap[expr] = pathElementsColumn(path, mlir::storage::PathElementsKind::Relationships);
        } else {
            throwError(fmt::format("{}() does not read a path", funcName), expr);
        }

        return;
    }

    if (funcName == "id") {
        if (!args || args->size() != 1) {
            throwError("id() expects 1 argument.", expr);
        }

        const mlir::Value input = translateArg(args->front());
        _part._exprMap[expr] = _opBuilder.create<mlir::db::ElementID>(loc, input).getResult();
        return;
    }

    // datetime() reads the clock rather than converting an argument, so it is the one
    // name whose call with no argument is a different op from its call with one.
    const bool namesTheClock = funcName == "datetime" && (!args || args->empty());
    if (namesTheClock) {
        const mlir::db::ColumnType dateTimeType = allocColumnType(mlir::storage::DateTimeType::get(_mlirCtxt));

        _part._exprMap[expr] = _opBuilder.create<mlir::db::CurrentDateTime>(loc, dateTimeType).getResult();
        return;
    }

    if (funcName == "pi") {
        _part._exprMap[expr] = constantDouble(std::numbers::pi);
        return;
    } else if (funcName == "e") {
        _part._exprMap[expr] = constantDouble(std::numbers::e);
        return;
    }

    const auto unaryIt = unaryFunctionEmitters.find(funcName);
    if (unaryIt != end(unaryFunctionEmitters)) {
        if (!args || args->size() != 1) {
            throwError(fmt::format("{}() expects 1 argument.", funcName), expr);
        }

        const mlir::Value input = translateArg(args->front());
        _part._exprMap[expr] = unaryIt->second(_opBuilder, loc, input);
        return;
    }

    const auto binaryIt = binaryFunctionEmitters.find(funcName);
    if (binaryIt != end(binaryFunctionEmitters)) {
        if (!args || args->size() != 2) {
            throwError(fmt::format("{}() expects 2 arguments.", funcName), expr);
        }

        const mlir::Value lhs = translateArg(args->getExprs()[0]);
        const mlir::Value rhs = translateArg(args->getExprs()[1]);
        _part._exprMap[expr] = binaryIt->second(_opBuilder, loc, noneType, lhs, rhs);
        return;
    }

    throwError(fmt::format("Unsupported function: {}", funcName), expr);
}

void DBProgramGenerator::translatePointDistance(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 2, "point.distance() takes 2 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value lhs = translateArg(argExprs[0]);
    const mlir::Value rhs = translateArg(argExprs[1]);

    mlir::db::PointDistance distance = _opBuilder.create<mlir::db::PointDistance>(_opBuilder.getUnknownLoc(),
                                                                                 allocColumnType(_opBuilder.getF64Type()),
                                                                                 lhs,
                                                                                 rhs);

    _part._exprMap[expr] = distance.getResult();
}

void DBProgramGenerator::translatePointWithinBBox(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 3, "point.withinBBox() takes 3 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value point = translateArg(argExprs[0]);
    const mlir::Value lowerLeft = translateArg(argExprs[1]);
    const mlir::Value upperRight = translateArg(argExprs[2]);

    const mlir::Type boolType = mlir::storage::BoolType::get(_mlirCtxt);

    mlir::db::PointWithinBBox within = _opBuilder.create<mlir::db::PointWithinBBox>(_opBuilder.getUnknownLoc(),
                                                                                   allocColumnType(boolType),
                                                                                   point,
                                                                                   lowerLeft,
                                                                                   upperRight);

    _part._exprMap[expr] = within.getResult();
}

void DBProgramGenerator::translateRange(const Expr* expr, const ExprChain* args) {
    const size_t argCount = args ? args->size() : 0;
    const bool boundsGiven = argCount == 2 || argCount == 3;
    if (!boundsGiven) {
        throwError("range() expects 2 or 3 arguments.", expr);
    }

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value start = translateArg(argExprs[0]);
    const mlir::Value end = translateArg(argExprs[1]);
    const mlir::Value step = argExprs.size() == 3 ? translateArg(argExprs[2]) : mlir::Value {};

    const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, _opBuilder.getI64Type());

    mlir::db::Range range = _opBuilder.create<mlir::db::Range>(_opBuilder.getUnknownLoc(),
                                                               allocColumnType(listType),
                                                               start,
                                                               end,
                                                               step);

    _part._exprMap[expr] = range.getResult();
}

void DBProgramGenerator::translateSubstring(const Expr* expr, const ExprChain* args) {
    bioassert(args && (args->size() == 2 || args->size() == 3), "substring() takes 2 or 3 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);
    const mlir::Value start = translateArg(argExprs[1]);
    const mlir::Value length = argExprs.size() == 3 ? translateArg(argExprs[2]) : mlir::Value {};

    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);

    mlir::db::Substring substring = _opBuilder.create<mlir::db::Substring>(_opBuilder.getUnknownLoc(),
                                                                           allocColumnType(stringType),
                                                                           string,
                                                                           start,
                                                                           length);

    _part._exprMap[expr] = substring.getResult();
}

void DBProgramGenerator::translateSortMulti(const Expr* expr, const ExprChain* args) {
    bioassert(args && !args->empty() && args->size() <= 4, "coll.sortMulti() takes 1 to 4 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();
    const size_t argCount = argExprs.size();

    const mlir::Value list = translateArg(argExprs[0]);
    const mlir::Value fields = argCount > 1 ? translateArg(argExprs[1]) : mlir::Value {};
    const mlir::Value limit = argCount > 2 ? translateArg(argExprs[2]) : mlir::Value {};
    const mlir::Value skip = argCount > 3 ? translateArg(argExprs[3]) : mlir::Value {};

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    mlir::db::SortMulti sortMulti = _opBuilder.create<mlir::db::SortMulti>(_opBuilder.getUnknownLoc(),
                                                                           noneType,
                                                                           list,
                                                                           fields,
                                                                           limit,
                                                                           skip);

    _part._exprMap[expr] = sortMulti.getResult();
}

void DBProgramGenerator::translateSplit(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 2, "split() takes 2 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);
    const mlir::Value delimiter = translateArg(argExprs[1]);

    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);
    const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, stringType);

    mlir::db::Split split = _opBuilder.create<mlir::db::Split>(_opBuilder.getUnknownLoc(),
                                                               allocColumnType(listType),
                                                               string,
                                                               delimiter);

    _part._exprMap[expr] = split.getResult();
}

template <typename StringEndOp>
void DBProgramGenerator::translateStringEnd(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 2, "left() and right() take 2 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);
    const mlir::Value length = translateArg(argExprs[1]);

    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);

    StringEndOp stringEnd = _opBuilder.create<StringEndOp>(_opBuilder.getUnknownLoc(),
                                                           allocColumnType(stringType),
                                                           string,
                                                           length);

    _part._exprMap[expr] = stringEnd.getResult();
}

void DBProgramGenerator::translateReplace(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 3, "replace() takes 3 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);
    const mlir::Value search = translateArg(argExprs[1]);
    const mlir::Value replacement = translateArg(argExprs[2]);

    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);

    mlir::db::Replace replace = _opBuilder.create<mlir::db::Replace>(_opBuilder.getUnknownLoc(),
                                                                     allocColumnType(stringType),
                                                                     string,
                                                                     search,
                                                                     replacement);

    _part._exprMap[expr] = replace.getResult();
}

template <typename NormalFormOp>
void DBProgramGenerator::translateNormalFormFunction(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 2, "A normal form function takes a string and a normal form.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);

    bioassert(argExprs[1]->getType() == EvaluatedType::NormalForm, "A normal form function reads its normal form from a literal.");

    const LiteralExpr* formExpr = static_cast<const LiteralExpr*>(argExprs[1]);
    const NormalFormLiteral* formLiteral = static_cast<const NormalFormLiteral*>(formExpr->getLiteral());
    const mlir::storage::UnicodeNormalForm form = toStorageNormalForm(formLiteral->getForm());

    NormalFormOp normalFormOp = _opBuilder.create<NormalFormOp>(_opBuilder.getUnknownLoc(), string, form);

    _part._exprMap[expr] = normalFormOp.getResult();
}
