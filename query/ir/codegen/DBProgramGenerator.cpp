#include "DBProgramGenerator.h"

#include <algorithm>
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
#include "StorageTypes.h"
#include "IRConstantColumn.h"
#include "ExplainReport.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "CypherAST.h"
#include "DiagnosticsManager.h"
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
