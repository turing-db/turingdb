#include "DBProgramGenerator.h"

#include <algorithm>
#include <memory>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

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
#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "stmt/ReturnStmt.h"

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
        // source variable is expliclty filtered by the edge op
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

    std::unordered_set<const VariableDependency*> defined;

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

void DBProgramGenerator::translateComponent(const VariableDependency* root,
                                            std::unordered_set<const VariableDependency*>& defined,
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
            markDefined(var); // Mark as defined to avoid retraversal
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
    const size_t count = components.size();
    bioassert(count >= 2, "Cross product cascade needs at least two components");

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    mlir::Block* currentTarget = targetBlock;

    mlir::Block* pendingYieldBlock = nullptr;

    for (size_t i = 0; i + 1 < count; i++) {
        llvm::SmallVector<mlir::Type> resultTypes;
        for (size_t j = i; j < count; j++) {
            for (const mlir::Value column : components[j]._columns) {
                resultTypes.push_back(column.getType());
            }
        }

        _opBuilder.setInsertionPointToEnd(currentTarget);
        auto crossProduct = _opBuilder.create<mlir::db::CrossProduct>(loc, resultTypes);

        mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();
        mlir::Block* const rightBlock = &crossProduct.getRightFactor().front();

        moveComponentToFactor(components[i], leftBlock);

        const mlir::ResultRange crossResults = crossProduct.getResults();
        if (pendingYieldBlock) {
            _opBuilder.setInsertionPointToEnd(pendingYieldBlock);
            _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {crossResults});
        } else {
            results.assign(crossResults.begin(), crossResults.end());
        }

        const bool lastPair = i + 2 == count;
        if (lastPair) {
            moveComponentToFactor(components[i + 1], rightBlock);
        } else {
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

    const auto lookup = [&](auto&& item) -> mlir::Value {
        using Type = std::remove_cvref_t<decltype(item)>;

        const VarDecl* var = nullptr;
        if constexpr (std::is_same_v<Type, Expr* >) {
            var = item->getExprVarDecl();
        } else {
            var = item;
        }
        bioassert(var, "Could not determine return variable.");

        const std::string_view name = var->getName();
        const auto findIt = finalIdentities.find(name);
        bioassert(findIt != end(finalIdentities), "Return item {} not found.", name);

        const mlir::Value mlirCol = findIt->second;
        return mlirCol;
    };

    llvm::SmallVector<mlir::Value> outputted;
    for (const Projection::ReturnItem item : returned) {
        const mlir::Value itemCol = std::visit(lookup, item);
        outputted.push_back(itemCol);
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    _opBuilder.create<mlir::db::Output>(loc, mlir::ValueRange{outputted});
}
