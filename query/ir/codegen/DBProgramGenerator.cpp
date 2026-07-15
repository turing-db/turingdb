#include "DBProgramGenerator.h"

#include <algorithm>
#include <memory>
#include <string_view>
#include <type_traits>

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
#include "llvm/ADT/SmallVector.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBTypes.h"
#include "StorageDialect.h"
#include "StorageTypes.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "CypherAST.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "WhereClause.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/LiteralExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace db;

namespace {

bool producesEdgeVar(const DependencyEdge* e) {
    const EdgeMetadata::EdgeType producedType = e->data().type();
    const bool getOut = producedType == EdgeMetadata::EdgeType::GET_OUT_EDGES;
    const bool getIn = producedType == EdgeMetadata::EdgeType::GET_IN_EDGES;
    return getOut || getIn;
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

        default:
            throw FatalException("Invalid attempt to reverse direction");
        break;
    }

    throw FatalException("Uncaught edge type.");

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

template<typename EdgeOp>
void DBProgramGenerator::addEdgeTraversal(const VariableDependency* src,
                                          const VariableDependency* edge,
                                          const VariableDependency* tgt,
                                          const std::vector<const VariableDependency*>& carrySet) {
    static_assert(std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetInEdges>, "Invalid op");

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

    const auto loc = _opBuilder.getUnknownLoc();
    auto op = _opBuilder.create<EdgeOp>(loc, results, operands);

    const mlir::Value newSrcs = op.getResult(0);
    const mlir::Value newEdges = op.getResult(1);
    // TODO: Register opResult(2): the edge types
    const mlir::Value newTgts = op.getResult(3);

    if constexpr (std::is_same_v<EdgeOp, mlir::db::GetOutEdges>) {
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
    generatePropertyConstraints(ast);
    generateFilters(ast);
    generateOutput(ast);

    _opBuilder.create<mlir::func::ReturnOp>(uloc);
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

    llvm::SmallVector<mlir::Value> columnsToFilter;
    llvm::SmallVector<const VariableDependency*> orderedVars;
    for (auto& [mapVar, values] : _varMap) {
        columnsToFilter.push_back(values.back());
        orderedVars.push_back(mapVar);
    }
    // Use the filtered column of the first source (arbitrary) as the value for the merged
    // variable
    columnsToFilter.push_back(fstSourceCol);
    orderedVars.push_back(mergeVar);

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : columnsToFilter) {
        resultTypes.push_back(column.getType());
    }

    auto fOp =
        _opBuilder.create<mlir::db::FilterOp>(uloc, resultTypes, eqRes, columnsToFilter);

    for (size_t index = 0; index < orderedVars.size(); index++) {
        registerValue(orderedVars[index], fOp.getResult(index));
    }
    carriedSet.push_back(mergeVar);
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

    addScanNodes(root);
    markDefined(root);

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

        // Have we found a (source, edge, target) triple yet on this traversal?
        const bool haveTriple = pred && predPred;

        for (const DependencyEdge* e : var->edges()) {
            const VariableDependency* other = e->src() == var ? e->tgt() : e->src();
            if (defined.contains(other)) {
                continue;
            }

            if (haveTriple) {
                // We have discovered a full (src, edge, tgt) triple, set the next
                // elements on the stack to only have (src, edge) and await tgt
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
                throw TuringException("Undirected edges not yet supported.");
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

void DBProgramGenerator::moveComponentToFactor(TranslatedComponent& component, mlir::Block* factorBlock) {
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
    const Projection* proj = rtn->getProjection();

    const bool all = proj->isReturningAll();
    // FIXME: Detect the unused MLIR vars and return those
    bioassert(!all, "Returning all is not yet supported.");

    const Projection::Items& returned = proj->items();

    std::unordered_map<std::string_view, mlir::Value> finalIdentities;
    for (auto& [cypherVar, mlirCol] : _varMap) {
        const std::string_view varName = cypherVar->getName();

        bioassert(not mlirCol.empty(), "No definitions for {}", varName);
        const mlir::Value finalValue = mlirCol.back();

        finalIdentities[varName] = finalValue;
    }

    const auto getVarForItem = [&](auto&& item) -> mlir::Value {
        using Type = std::remove_cvref_t<decltype(item)>;

        if constexpr (std::is_same_v<Type, VarDecl*>) {
            const std::string_view name = item->getName();
            const auto findIt = finalIdentities.find(name);
            bioassert(findIt != end(finalIdentities), "Return variable '{}' not found", name);
            return findIt->second;
        } else {
            const VarDecl* var = item->getExprVarDecl();
            if (var) {
                const std::string_view name = var->getName();
                const auto findIt = finalIdentities.find(name);
                if (findIt != end(finalIdentities)) {
                    return findIt->second;
                }
            }

            translateExpr(item);
            return _exprMap.at(item);
        }
    };

    llvm::SmallVector<mlir::Value> outputted;
    for (const Projection::ReturnItem item : returned) {
        const mlir::Value itemCol = std::visit(getVarForItem, item);
        outputted.push_back(itemCol);
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    _opBuilder.create<mlir::db::Output>(loc, mlir::ValueRange{outputted});
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

    for (const Stmt* stmt : stmtsContainer->stmts()) {
        if (stmt->getKind() != Stmt::Kind::MATCH) {
            continue;
        }

        const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
        const Pattern* pattern = matchStmt->getPattern();

        // Collect all property constraint expressions from every entity pattern
        // in this MATCH. Each EntityPropertyConstraint._expr is a synthesized
        // equality BinaryExpr (e.g. n.name = "Alice") produced by the analyzer.
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

        if (constraintExprs.empty()) {
            continue;
        }

        const mlir::Location loc = _opBuilder.getUnknownLoc();
        const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

        // Fold left on all expressions, combining with AND
        mlir::Value combinedPredicate;
        for (const Expr* constraintExpr : constraintExprs) {
            translateExpr(constraintExpr);
            const mlir::Value predicate = _exprMap.at(constraintExpr);

            if (!combinedPredicate) {
                combinedPredicate = predicate;
            } else {
                combinedPredicate = _opBuilder.create<mlir::db::AndOp>(loc, boolType, combinedPredicate, predicate).getResult();
            }
        }

        llvm::SmallVector<mlir::Value> columnsToFilter;
        llvm::SmallVector<const VariableDependency*> orderedVars;
        for (auto& [var, values] : _varMap) {
            columnsToFilter.push_back(values.back());
            orderedVars.push_back(var);
        }

        if (columnsToFilter.empty()) {
            continue;
        }

        llvm::SmallVector<mlir::Type> resultTypes;
        for (const mlir::Value column : columnsToFilter) {
            resultTypes.push_back(column.getType());
        }

        auto filterOp = _opBuilder.create<mlir::db::FilterOp>(
            loc, resultTypes, combinedPredicate, columnsToFilter);

        for (size_t index = 0; index < orderedVars.size(); index++) {
            const VariableDependency* var = orderedVars[index];
            _varMap.at(var).push_back(filterOp.getResult(index));
        }
    }
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

    // All three below reused over iterations
    // Filter all columns that are defined here
    llvm::SmallVector<mlir::Value> columnsToFilter;
    // Track the variables to insert the new filtered values into the map
    llvm::SmallVector<const VariableDependency*> orderedVars;
    // Result types are the same as the input types
    llvm::SmallVector<mlir::Type> resultTypes;
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

        const Expr* predicateExpr = where->getExpr();
        translateExpr(predicateExpr);

        const auto findIt = _exprMap.find(predicateExpr);
        bioassert(findIt != end(_exprMap), "Failed to get value for expr");

        const mlir::Value predicate = findIt->second;
        const mlir::Location loc = _opBuilder.getUnknownLoc();

        columnsToFilter.clear();
        orderedVars.clear();
        for (auto& [var, values] : _varMap) {
            columnsToFilter.push_back(values.back());
            orderedVars.push_back(var);
        }

        if (columnsToFilter.empty()) {
            continue;
        }

        resultTypes.clear();
        for (const mlir::Value column : columnsToFilter) {
            resultTypes.push_back(column.getType());
        }

        mlir::db::FilterOp filterOp = _opBuilder.create<mlir::db::FilterOp>(
            loc, resultTypes, predicate, columnsToFilter);

        for (size_t index = 0; index < orderedVars.size(); index++) {
            const VariableDependency* var = orderedVars[index];
            const mlir::Value result = filterOp.getResult(index);
            _varMap.at(var).push_back(result);
        }
    }
}

void DBProgramGenerator::translateExpr(const Expr* expr) {
    bioassert(!_exprMap.contains(expr), "Attempted to retranslate expr.");

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
            const std::string_view varName = symbolExpr->getDecl()->getName();

            for (const auto& [var, values] : _varMap) {
                if (var->getName() == varName) {
                    _exprMap[expr] = values.back();
                    break;
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

        case Expr::Kind::FUNCTION_INVOCATION:
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

        case UnaryOperator::Minus:
        case UnaryOperator::Plus:
            throw TuringException(fmt::format("Unsupported unary operator: {}",
                                              UnaryOperatorDescription::value(op)));
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

        case BinaryOperator::Xor:
        case BinaryOperator::NotEqual:
        case BinaryOperator::Mod:
        case BinaryOperator::Pow:
        case BinaryOperator::In:
            throw TuringException(fmt::format("Unsupported operation: {}",
                                              BinaryOperatorDescription::value(op)));
        break;

        case BinaryOperator::_SIZE:
        break;
    }
}

mlir::Value DBProgramGenerator::translateLiteralExpr(const Literal* literal) {
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    mlir::TypedAttr valueAttr;

    switch (literal->getKind()) {
        case Literal::Kind::BOOL: {
            const BoolLiteral* boolLiteral = static_cast<const BoolLiteral*>(literal);
            valueAttr = _opBuilder.getBoolAttr(boolLiteral->getValue());
        }
        break;
        case Literal::Kind::INTEGER: {
            const IntegerLiteral* intLiteral = static_cast<const IntegerLiteral*>(literal);
            valueAttr = _opBuilder.getI64IntegerAttr(intLiteral->getValue());
        }
        break;
        case Literal::Kind::DOUBLE: {
            const DoubleLiteral* doubleLiteral = static_cast<const DoubleLiteral*>(literal);
            valueAttr = _opBuilder.getF64FloatAttr(doubleLiteral->getValue());
        }
        break;

        case Literal::Kind::STRING: {
            const StringLiteral* stringLiteral = static_cast<const StringLiteral*>(literal);
            const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);
            valueAttr = mlir::StringAttr::get(stringLiteral->getValue(), stringType);
        }
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
            throw FatalException("List literals are not yet supported in MLIR codegen.");
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

    mlir::Value entityColumn;
    for (const auto& [var, values] : _varMap) {
        if (var->getName() == varName) {
            entityColumn = values.back();
            break;
        }
    }
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
