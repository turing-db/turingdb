#include "DBProgramGenerator.h"

#include <algorithm>
#include <map>
#include <memory>
#include <numbers>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "EntityPattern.h"
#include "LabelExpr.h"
#include "expr/Operators.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/BitVector.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBColumnElement.h"
#include "DBDialect.h"
#include "DBOps.h"
#include "DBPassPipeline.h"
#include "DBPasses.h"
#include "DBTypes.h"
#include "DBSystemProgramGenerator.h"
#include "DBBoundColumns.h"
#include "DBHiddenColumnNames.h"
#include "DBSchemaNameAttributes.h"
#include "StorageAttributes.h"
#include "StorageDialect.h"
#include "StorageEnums.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"
#include "IRValueTypes.h"
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
#include "Symbol.h"
#include "stmt/CallStmt.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/StmtContainer.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/ParameterDecl.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "expr/BinaryExpr.h"
#include "expr/CaseExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/ExistsExpr.h"
#include "expr/CountSubqueryExpr.h"
#include "expr/CollectSubqueryExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/IndexExpr.h"
#include "expr/ListComprehensionExpr.h"
#include "expr/ListPredicateExpr.h"
#include "expr/ReduceExpr.h"
#include "expr/AllReduceExpr.h"
#include "expr/MapProjectionExpr.h"
#include "expr/ListSliceExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/LogicalExpr.h"
#include "expr/ParameterExpr.h"
#include "expr/PatternComprehensionExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/PropertyLookupExpr.h"
#include "expr/StringExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "stmt/CreateStmt.h"
#include "stmt/MergeStmt.h"
#include "stmt/NextSegmentStmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/ShortestPathStmt.h"
#include "stmt/UnwindStmt.h"
#include "stmt/VectorSearchStmt.h"
#include "stmt/WithStmt.h"
#include "SymbolChain.h"

#include "BioAssert.h"
#include "FatalException.h"

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

// The branches of @param body when it is a UNION, and @param body itself otherwise
void collectUnionBranches(const RegularQuery* body, llvm::SmallVectorImpl<const RegularQuery*>& branches) {
    if (body->getKind() != QueryCommand::Kind::UNION_QUERY) {
        branches.push_back(body);
        return;
    }

    for (const UnionQuery::Branch& branch : static_cast<const UnionQuery*>(body)->branches()) {
        branches.push_back(branch._query);
    }
}

// The queries whose rows add up to the rows of @param body: the branches of a UNION ALL, and
// @param body itself otherwise
void collectUnionAllBranches(const RegularQuery* body, llvm::SmallVectorImpl<const RegularQuery*>& branches) {
    const bool unionAll = body->getKind() == QueryCommand::Kind::UNION_QUERY
                       && static_cast<const UnionQuery*>(body)->getDedupedBranchCount() == 0;

    if (unionAll) {
        collectUnionBranches(body, branches);
    } else {
        branches.push_back(body);
    }
}

mlir::storage::ListPredicateKind listPredicateKind(ListPredicateExpr::Quantifier quantifier) {
    switch (quantifier) {
        case ListPredicateExpr::Quantifier::All:
            return mlir::storage::ListPredicateKind::All;
        break;

        case ListPredicateExpr::Quantifier::Any:
            return mlir::storage::ListPredicateKind::Any;
        break;

        case ListPredicateExpr::Quantifier::None:
            return mlir::storage::ListPredicateKind::None;
        break;

        case ListPredicateExpr::Quantifier::Single:
            return mlir::storage::ListPredicateKind::Single;
        break;
    }

    return mlir::storage::ListPredicateKind::All;
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

// The type a list of @param shape is read under, as a literal of that shape infers it: the
// leaf's own type, none for a leaf no element names, any where the elements share none,
// under one list per level
mlir::Type listShapeType(mlir::OpBuilder& builder, const ListShape& shape) {
    mlir::MLIRContext* const context = builder.getContext();
    const EvaluatedType leaf = shape.getLeafType();
    const bool sharesNoType = leaf == EvaluatedType::Invalid;
    const std::optional<ValueType> leafValueType = sharesNoType ? std::nullopt : toValueType(leaf);

    mlir::Type element;
    if (leaf == EvaluatedType::Null) {
        element = mlir::NoneType::get(context);
    } else if (leafValueType) {
        element = valueTypeToElementType(builder, *leafValueType);
    } else {
        element = mlir::storage::AnyType::get(context);
    }

    for (size_t level = 0; level < shape.getDepth(); level++) {
        element = mlir::storage::ListType::get(context, element);
    }

    return element;
}

// The element type of the lists a db.make_list builds: the one type its element columns
// name, or the type-erased tagged scalar where they name no single one. A column whose own
// type is resolved during lowering names `none`, and no verdict can be taken against a
// type that is not known yet: the whole list stays `none` and lowering takes the verdict
// over the resolved chunks.
mlir::Type sharedColumnElement(mlir::MLIRContext* context, llvm::ArrayRef<mlir::Value> columns) {
    mlir::Type shared;

    for (const mlir::Value column : columns) {
        const mlir::Type element = mlir::cast<mlir::db::ColumnType>(column.getType()).getType();

        if (mlir::isa<mlir::NoneType>(element)) {
            return element;
        }

        if (!shared) {
            shared = element;
        } else if (shared != element) {
            return mlir::storage::AnyType::get(context);
        }
    }

    return shared;
}

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

void DBProgramGenerator::translateListElements(const ListLiteral* list,
                                               llvm::SmallVectorImpl<mlir::Attribute>& elements) {
    const ListLiteral::Items& items = list->items();
    elements.reserve(items.size());

    for (const Expr* item : items) {
        const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(item);
        if (!literalExpr) {
            throwError("Only literal elements are supported in a list.", item);
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
    } else if (kind == Literal::Kind::MAP) {
        const MapLiteral* nested = static_cast<const MapLiteral*>(literal);

        mlir::NamedAttrList nestedEntries;
        translateMapEntries(nested, nestedEntries);

        return nestedEntries.getDictionary(_mlirCtxt);
    }

    return scalarLiteralAttr(literal);
}

mlir::Attribute DBProgramGenerator::listElementAttr(const Literal* literal) {
    const mlir::Attribute element = literalAttr(literal);

    if (!element) {
        throwError("Only booleans, integers, floats, strings, nulls, embeddings, maps and "
                   "lists are supported as list elements.",
                   literal);
    }

    return element;
}

mlir::Value DBProgramGenerator::translateListLiteral(const ListLiteral* list) {
    if (!list->isLiteralTree()) {
        return translateListOfColumns(list);
    }

    llvm::SmallVector<mlir::Attribute> elements;
    translateListElements(list, elements);

    // A list literal is a constant like any other, carried as the array of its elements.
    // The column type is db.constant's to infer: an array attribute has none of its own, so
    // the op reads the homogeneity verdict off the elements.
    mlir::db::ConstantOp constant = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                           _opBuilder.getArrayAttr(elements));

    return constant.getResult();
}

mlir::Value DBProgramGenerator::joinedListOperand(mlir::Value column) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::Type elementType = mlir::cast<mlir::db::ColumnType>(column.getType()).getType();
    const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, elementType);
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    mlir::db::MakeList makeList = _opBuilder.create<mlir::db::MakeList>(loc,
                                                                        allocColumnType(listType),
                                                                        mlir::ValueRange {column});

    const mlir::Value holdsAValue = _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, column).getResult();

    return createCase(mlir::ValueRange {holdsAValue}, mlir::ValueRange {makeList.getResult()}, mlir::Value {});
}

mlir::Value DBProgramGenerator::translateListOfColumns(const ListLiteral* list) {
    llvm::SmallVector<mlir::Value> elementColumns;
    for (const Expr* item : list->items()) {
        elementColumns.push_back(getOrTranslateExprColumn(item));
    }

    const mlir::Type elementType = sharedColumnElement(_mlirCtxt, elementColumns);
    const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, elementType);

    mlir::db::MakeList makeList = _opBuilder.create<mlir::db::MakeList>(_opBuilder.getUnknownLoc(),
                                                                        allocColumnType(listType),
                                                                        mlir::ValueRange {elementColumns});

    return makeList.getResult();
}

mlir::Value DBProgramGenerator::translateMapLiteral(const MapLiteral* map) {
    if (!map->isLiteralTree()) {
        return translateMapOfColumns(map);
    }

    mlir::NamedAttrList entries;
    translateMapEntries(map, entries);

    // DictionaryAttr canonicalises by sorting on key name, so the entries reach the
    // interpreter sorted rather than in the order MapLiteral holds them.
    mlir::db::ConstantOp constant = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                            entries.getDictionary(_mlirCtxt));

    return constant.getResult();
}

mlir::Value DBProgramGenerator::translateMapOfColumns(const MapLiteral* map) {
    const std::span<const MapLiteral::Entry> heldEntries = map->getHeldEntries();

    llvm::SmallVector<std::pair<std::string_view, const Expr*>> entries;
    entries.reserve(heldEntries.size());

    for (const auto& [key, valueExpr] : heldEntries) {
        entries.emplace_back(key->getName(), valueExpr);
    }

    return emitMakeMap(entries);
}

mlir::Value DBProgramGenerator::emitMakeMap(std::span<const std::pair<std::string_view, const Expr*>> entries) {
    if (entries.empty()) {
        mlir::db::ConstantOp empty = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                             _opBuilder.getDictionaryAttr({}));
        return empty.getResult();
    }

    // Sorted on key name, as the DictionaryAttr of a constant map is
    std::map<std::string_view, mlir::Value> columnsByKey;
    for (const auto& [keyName, valueExpr] : entries) {
        columnsByKey.emplace(keyName, getOrTranslateExprColumn(valueExpr));
    }

    llvm::SmallVector<mlir::Attribute> keys;
    llvm::SmallVector<mlir::Value> valueColumns;
    keys.reserve(columnsByKey.size());
    valueColumns.reserve(columnsByKey.size());

    for (const auto& [keyName, column] : columnsByKey) {
        keys.push_back(_opBuilder.getStringAttr(llvm::StringRef(keyName.data(), keyName.size())));
        valueColumns.push_back(column);
    }

    mlir::db::MakeMap makeMap = _opBuilder.create<mlir::db::MakeMap>(_opBuilder.getUnknownLoc(),
                                                                     mlir::ValueRange {valueColumns},
                                                                     _opBuilder.getArrayAttr(keys));

    return makeMap.getResult();
}

void DBProgramGenerator::translateMapEntries(const MapLiteral* map, mlir::NamedAttrList& entries) {
    const std::span<const MapLiteral::Entry> heldEntries = map->getHeldEntries();
    entries.reserve(heldEntries.size());

    for (const auto& [key, valueExpr] : heldEntries) {
        const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(valueExpr);

        if (!literalExpr) {
            throwError("Only literal values are supported in a map.", valueExpr);
        }

        const Literal* literal = literalExpr->getLiteral();
        const mlir::Attribute value = literalAttr(literal);

        if (!value) {
            throwError("Only booleans, integers, floats, strings, nulls, embeddings, "
                       "lists and maps are supported as map values.",
                       literal);
        }

        const std::string_view keyName = key->getName();
        entries.append(_opBuilder.getStringAttr(llvm::StringRef(keyName.data(), keyName.size())), value);
    }
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

// Each branch is generated into a region of its own, as a query body in its own right:
// the branches share no variable, so the scope and the dependency graph are dropped
// between them the way a barrier drops them.
void DBProgramGenerator::generateUnion(const UnionQuery* unionQuery) {
    const UnionQuery::Branches& branches = unionQuery->branches();
    const size_t dedupedBranches = unionQuery->getDedupedBranchCount();

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    mlir::Value distinctSet;
    if (dedupedBranches > 0) {
        distinctSet = _opBuilder.create<mlir::db::DistinctSet>(loc).getSet();
    }

    mlir::db::Union unionOp = _opBuilder.create<mlir::db::Union>(loc, branches.size());

    for (size_t branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
        _part = PartScope {};
        _vdg.clear();

        _opBuilder.setInsertionPointToStart(&unionOp.getBranches()[branchIndex].front());

        const bool dedupsWithTheBranchesBeforeIt = branchIndex < dedupedBranches;
        const UnionBranch branch {dedupsWithTheBranchesBeforeIt ? distinctSet : mlir::Value()};

        const RegularQuery* branchQuery = branches[branchIndex]._query;

        if (branchQuery->getKind() == QueryCommand::Kind::SINGLE_PART_QUERY) {
            generateQuery(static_cast<const SinglePartQuery*>(branchQuery), &branch);
        } else {
            generateBodyOutput(branchQuery, &branch);
        }
    }

    _opBuilder.setInsertionPointAfter(unionOp);
}

void DBProgramGenerator::generateBodyOutput(const RegularQuery* body, const UnionBranch* branch) {
    const BodyScopes scopes;

    llvm::SmallVector<PublishedColumn> yielded;
    CarriedEntities returnedEntities;
    generateSubqueryBody(body, scopes, /*answersRows=*/false, yielded, returnedEntities);

    const ReturnStmt* returnStmt = body->getFirstSingleQuery()->getReturnStmt();
    if (!returnStmt) {
        return;
    }

    llvm::SmallVector<mlir::Value> columns;
    llvm::SmallVector<llvm::StringRef> names;
    for (const VarDecl* decl : returnStmt->getProjection()->publishedDecls()) {
        const auto columnIt = std::ranges::find(yielded, decl, &PublishedColumn::_decl);
        bioassert(columnIt != yielded.end(), "Column '{}' not yielded by the branches", decl->getName());

        columns.push_back(columnIt->_column);
        names.push_back(columnIt->_name);
    }

    if (branch) {
        broadcastUnionProjection(columns);

        if (branch->_distinctSet) {
            dedupUnionBranch(branch->_distinctSet, columns);
        }
    }

    _opBuilder.create<mlir::db::Output>(_opBuilder.getUnknownLoc(),
                                       mlir::ValueRange {columns},
                                       _opBuilder.getStrArrayAttr(names));
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

mlir::Value DBProgramGenerator::checkNodeLabels(mlir::Value nodeColumn,
                                                std::span<const LabelRef> labels,
                                                bool nullable) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType labelSetIDType =
        allocColumnType(mlir::storage::LabelSetIDType::get(_mlirCtxt));
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const mlir::Value labelSetIDColumn = _opBuilder.create<mlir::db::GetNodeLabelSet>(
        loc,
        labelSetIDType,
        nodeColumn).getResult();

    const mlir::ArrayAttr alternatives = _opBuilder.getArrayAttr({schemaNamesAttr(_opBuilder, labels)});

    return _opBuilder.create<mlir::db::CheckLabelConstraint>(
        loc,
        boolType,
        labelSetIDColumn,
        alternatives,
        nullable).getResult();
}

mlir::Value DBProgramGenerator::checkEdgeType(mlir::Value edgeTypeColumn,
                                              std::span<const EdgeTypeRef> edgeTypes,
                                              bool nullable) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    return _opBuilder.create<mlir::db::CheckEdgeTypeConstraint>(
        loc,
        boolType,
        edgeTypeColumn,
        schemaNamesAttr(_opBuilder, edgeTypes),
        nullable).getResult();
}

mlir::Value DBProgramGenerator::checkNodeLabelExpr(mlir::Value nodeColumn,
                                                   const LabelExpr* labelExpr,
                                                   bool nullable) {
    const auto checkLabelSet = [this, nodeColumn, nullable](const SymbolChain* labels) {
        std::vector<LabelRef> refs;
        collectSchemaNames(labels, refs);

        return checkNodeLabels(nodeColumn, refs, nullable);
    };

    return combineLabelSetChecks(labelExpr, checkLabelSet);
}

mlir::Value DBProgramGenerator::checkEdgeTypeExpr(mlir::Value edgeTypeColumn,
                                                  const LabelExpr* labelExpr,
                                                  bool nullable) {
    const auto checkTypeSet = [this, edgeTypeColumn, nullable](const SymbolChain* types) {
        std::vector<EdgeTypeRef> refs;
        collectSchemaNames(types, refs);

        mlir::Value conjunction;
        for (const EdgeTypeRef& ref : refs) {
            const mlir::Value check = checkEdgeType(edgeTypeColumn, std::span<const EdgeTypeRef> {&ref, 1}, nullable);
            conjunction = conjunction ? translateLogicalOperator(LogicalOperator::And, conjunction, check) : check;
        }

        return conjunction;
    };

    return combineLabelSetChecks(labelExpr, checkTypeSet);
}

mlir::Value DBProgramGenerator::combineLabelSetChecks(const LabelExpr* labelExpr, LabelSetCheck checkLabelSet) {
    const LabelExpr::Operands& operands = labelExpr->getOperands();

    switch (labelExpr->getKind()) {
        case LabelExpr::Kind::LABELS:
            return checkLabelSet(labelExpr->getLabels());
        break;
        case LabelExpr::Kind::NOT: {
            const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
            const mlir::Value operand = combineLabelSetChecks(operands.front(), checkLabelSet);

            return _opBuilder.create<mlir::db::NotOp>(_opBuilder.getUnknownLoc(), boolType, operand).getResult();
        }
        break;
        case LabelExpr::Kind::AND:
        case LabelExpr::Kind::OR: {
            const LogicalOperator op = labelExpr->getKind() == LabelExpr::Kind::AND ? LogicalOperator::And
                                                                                     : LogicalOperator::Or;

            mlir::Value result;
            for (const LabelExpr* operandExpr : operands) {
                const mlir::Value operand = combineLabelSetChecks(operandExpr, checkLabelSet);
                result = result ? translateLogicalOperator(op, result, operand) : operand;
            }

            return result;
        }
        break;
    }

    throwError("Unknown label expression.");
}

mlir::Value DBProgramGenerator::resolveEdgeTypeColumn(const VarDecl* decl) const {
    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();

    const auto identityIt = edgeIdentities.find(decl);
    if (identityIt == edgeIdentities.end() || identityIt->second.empty()) {
        return mlir::Value {};
    }

    const auto findIt = _part._edgeTypeMap.find(identityIt->second.front());
    if (findIt == _part._edgeTypeMap.end() || !isRowAlignedHere(findIt->second)) {
        return mlir::Value {};
    }

    return findIt->second;
}

mlir::Value DBProgramGenerator::resolveOrFetchEdgeTypeColumn(const VarDecl* decl,
                                                             std::string_view varName) {
    const mlir::Value published = resolveEdgeTypeColumn(decl);
    if (published) {
        return published;
    }

    const mlir::Value edgeColumn = resolveEntityColumn(decl);
    bioassert(edgeColumn, "Type test on unknown edge variable: {}", varName);

    return fetchEdgeTypeColumn(edgeColumn);
}

mlir::Value DBProgramGenerator::fetchEdgeTypeColumn(mlir::Value edgeColumn) {
    const mlir::db::ColumnType edgeTypeIDType =
        allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));

    return _opBuilder.create<mlir::db::GetEdgeTypes>(
        _opBuilder.getUnknownLoc(),
        edgeTypeIDType,
        edgeColumn).getResult();
}

mlir::Value DBProgramGenerator::resolveProjectionDriver(llvm::ArrayRef<mlir::Value> projected) {
    for (const mlir::Value column : projected) {
        if (!yieldsConstantColumn(column, _constantColumns)) {
            return column;
        }
    }

    return resolveRowCarryingColumn();
}

void DBProgramGenerator::broadcastUnionProjection(llvm::SmallVectorImpl<mlir::Value>& projected) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    const mlir::Value driver = resolveProjectionDriver(projected);

    for (mlir::Value& column : projected) {
        if (yieldsConstantColumn(column, _constantColumns)) {
            column = _opBuilder.create<mlir::db::BroadcastConstant>(loc, column, driver).getResult();
        }
    }
}

// The rows a branch contributes, recorded in the set its siblings share, so that the
// union tells apart rows reaching the result through different branches. Every column is
// part of the key here - unlike RETURN DISTINCT, which leaves a constant one out: `RETURN
// n.name, 1` and `RETURN m.name, 2` are distinct rows, and the constant is what says so.
void DBProgramGenerator::dedupUnionBranch(mlir::Value distinctSet,
                                          llvm::SmallVectorImpl<mlir::Value>& projected) {
    llvm::SmallVector<mlir::Value> deduped;
    emitRemoveDuplicates(projected, distinctSet, deduped);

    projected.assign(deduped.begin(), deduped.end());
}

void DBProgramGenerator::generateNextSegment(const NextSegmentStmt* segment) {
    const CallSubqueryStmt* body = segment->getBody();

    if (!body->isReturning()) {
        generateUpdatingSegment(segment);
    } else if (body->getBody()->getKind() == QueryCommand::Kind::CONDITIONAL_QUERY) {
        generateConditionalSegment(segment);
    } else {
        generateUnionSegment(segment);
    }
}

void DBProgramGenerator::generateUnionSegment(const NextSegmentStmt* segment) {
    const UnionQuery* unionQuery = static_cast<const UnionQuery*>(segment->getBody()->getBody());
    const UnionQuery::Branches& branches = unionQuery->branches();
    const size_t dedupedBranches = unionQuery->getDedupedBranchCount();
    mlir::Block* const outerBlock = _opBuilder.getInsertionBlock();

    SegmentTable table;
    materializeTable(table);

    mlir::Value distinctSet;
    if (dedupedBranches > 0) {
        distinctSet = _opBuilder.create<mlir::db::DistinctSet>(_opBuilder.getUnknownLoc()).getSet();
    }

    SubqueryBranchResults branchResults;
    BodyScopes scopes;

    for (const UnionQuery::Branch& branch : branches) {
        llvm::SmallVector<mlir::Value> scanned;
        scanTable(table, openSubqueryBranch(branchResults), scanned);

        collectBodyScopes(branch._query,
                          table._rowColumns,
                          scanned,
                          mlir::Value(),
                          table._constants,
                          /*bindsHidden=*/false,
                          scopes);
    }

    std::unordered_set<std::string_view> outerImportedCSVRows;
    std::swap(_importedCSVRows, outerImportedCSVRows);

    for (size_t branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
        mlir::Block* const block = &branchResults._regions[branchIndex]->front();
        generateSubqueryBranch(branches[branchIndex]._query,
                               scopes,
                               /*answersRows=*/false,
                               block,
                               block,
                               branchResults);
    }

    llvm::SmallVector<PublishedColumn> yielded;
    CarriedEntities returnedEntities;
    generateUnionOverBranches(distinctSet, dedupedBranches, outerBlock, branchResults, yielded, returnedEntities);

    std::swap(_importedCSVRows, outerImportedCSVRows);

    appendRedeclaredImports(segment->redeclaredImports(), yielded);

    rebindScope(yielded);
    bindReturnedEntities(returnedEntities);
}

// What a branch returns replaces the row, so a name the row held and the branch returns
// again is bound afresh rather than beside the input it came from
void DBProgramGenerator::generateConditionalSegment(const NextSegmentStmt* segment) {
    SubqueryResults results;
    emitCallSubquery(segment->getBody(), results);

    appendRedeclaredImports(segment->redeclaredImports(), results._returned);

    rebindScope(results._returned);
    bindReturnedEntities(results._returnedEntities);
}

// The segment after this one starts from a single row, so this one is the body of a
// CALL () run once over that row, reading the table it is handed through a scan
void DBProgramGenerator::generateUpdatingSegment(const NextSegmentStmt* segment) {
    const CallSubqueryStmt* body = segment->getBody();
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const outerBlock = _opBuilder.getInsertionBlock();

    SegmentTable table;
    materializeTable(table);

    mlir::Region bodyRegion;
    mlir::Block* const bodyBlock = new mlir::Block(); // Region destructor frees it
    bodyRegion.push_back(bodyBlock);

    llvm::SmallVector<mlir::Value> scanned;
    scanTable(table, bodyBlock, scanned);

    std::unordered_set<std::string_view> outerImportedCSVRows;
    std::swap(_importedCSVRows, outerImportedCSVRows);

    const RegularQuery* bodyQuery = body->getBody();

    if (bodyQuery->getKind() == QueryCommand::Kind::CONDITIONAL_QUERY) {
        llvm::SmallVector<PublishedColumn> scope;
        for (size_t columnIndex = 0; columnIndex < scanned.size(); columnIndex++) {
            const PublishedColumn& rowColumn = table._rowColumns[columnIndex];
            scope.push_back({rowColumn._decl, rowColumn._name, scanned[columnIndex]});
        }

        scope.append(table._constants.begin(), table._constants.end());

        _opBuilder.setInsertionPointToEnd(bodyBlock);
        rebindScopeKeepingWrittenEntities(scope);

        SubqueryResults results;
        emitCallSubquery(body, results);
    } else {
        const SinglePartQuery* query = static_cast<const SinglePartQuery*>(bodyQuery);

        BranchScope scope;
        collectSubqueryBranchScope(query,
                                   table._rowColumns,
                                   scanned,
                                   mlir::Value(),
                                   table._constants,
                                   /*bindsHidden=*/false,
                                   scope);

        _opBuilder.setInsertionPointToEnd(bodyBlock);
        rebindSubqueryBranchScope(scope);
        generateQueryParts(query);
    }

    _opBuilder.create<mlir::db::SubqueryYield>(loc, mlir::Value(), mlir::ValueRange {});

    std::swap(_importedCSVRows, outerImportedCSVRows);

    _opBuilder.setInsertionPointToEnd(outerBlock);

    mlir::db::CallSubquery callOp = _opBuilder.create<mlir::db::CallSubquery>(loc,
                                                                             mlir::TypeRange {},
                                                                             mlir::ValueRange {},
                                                                             /*unit=*/true,
                                                                             /*carriesScope=*/false,
                                                                             /*optional=*/false);
    callOp.getBody().takeBody(bodyRegion);

    llvm::SmallVector<PublishedColumn> redeclared;
    appendRedeclaredImports(segment->redeclaredImports(), redeclared);

    rebindScopeKeepingWrittenEntities(redeclared);
}

void DBProgramGenerator::materializeTable(SegmentTable& table) {
    llvm::SmallVector<PublishedColumn> scopeColumns;
    collectPublishedColumns(scopeColumns);

    splitConstantColumns(scopeColumns, table._rowColumns, table._constants);

    if (table._rowColumns.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Value> rowColumns;
    for (const PublishedColumn& rowColumn : table._rowColumns) {
        rowColumns.push_back(rowColumn._column);
    }

    const mlir::db::MaterializedType rowsType = mlir::db::MaterializedType::get(_mlirCtxt);
    table._rows = _opBuilder.create<mlir::db::Materialize>(_opBuilder.getUnknownLoc(), rowsType, rowColumns).getRows();
}

void DBProgramGenerator::scanTable(const SegmentTable& table,
                                   mlir::Block* block,
                                   llvm::SmallVectorImpl<mlir::Value>& scanned) {
    if (!table._rows) {
        return;
    }

    llvm::SmallVector<mlir::Type> columnTypes;
    for (const PublishedColumn& rowColumn : table._rowColumns) {
        columnTypes.push_back(rowColumn._column.getType());
    }

    _opBuilder.setInsertionPointToStart(block);
    mlir::db::ScanMaterialized scan = _opBuilder.create<mlir::db::ScanMaterialized>(_opBuilder.getUnknownLoc(),
                                                                                   columnTypes,
                                                                                   table._rows);

    const mlir::ResultRange columns = scan.getColumns();
    scanned.assign(columns.begin(), columns.end());
}

void DBProgramGenerator::generateSubqueryBody(const RegularQuery* body,
                                              const BodyScopes& scopes,
                                              bool answersRows,
                                              llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                              CarriedEntities& returnedEntities) {
    if (body->getKind() == QueryCommand::Kind::UNION_QUERY) {
        generateSubqueryUnion(static_cast<const UnionQuery*>(body), scopes, answersRows, yielded, returnedEntities);
    } else {
        generateSubqueryConditional(static_cast<const ConditionalQuery*>(body),
                                    scopes,
                                    answersRows,
                                    yielded,
                                    returnedEntities);
    }
}

// Each branch is generated into a region of its own under the imports it names, as the
// branches of a top-level UNION are. Every branch publishes its columns under the
// declarations the analyzer gave the first one, so they come out in the same order.
void DBProgramGenerator::generateSubqueryUnion(const UnionQuery* unionQuery,
                                               const BodyScopes& scopes,
                                               bool answersRows,
                                               llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                               CarriedEntities& returnedEntities) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const bodyBlock = _opBuilder.getInsertionBlock();
    const size_t dedupedBranches = unionQuery->getDedupedBranchCount();

    mlir::Value distinctSet;
    if (dedupedBranches > 0) {
        distinctSet = _opBuilder.create<mlir::db::DistinctSet>(loc).getSet();
    }

    SubqueryBranchResults branchResults;

    for (const UnionQuery::Branch& branch : unionQuery->branches()) {
        generateSubqueryBranch(branch._query,
                               scopes,
                               answersRows,
                               openSubqueryBranch(branchResults),
                               bodyBlock,
                               branchResults);
    }

    generateUnionOverBranches(distinctSet, dedupedBranches, bodyBlock, branchResults, yielded, returnedEntities);
}

void DBProgramGenerator::generateUnionOverBranches(mlir::Value distinctSet,
                                                   size_t dedupedBranches,
                                                   mlir::Block* bodyBlock,
                                                   SubqueryBranchResults& branchResults,
                                                   llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                                   CarriedEntities& returnedEntities) {
    const size_t branchCount = branchResults._regions.size();

    yieldEveryPendingMask(branchResults);

    // The branches share one distinct set, so each dedups on the same columns: its masks
    // included, once every branch yields them all
    for (size_t branchIndex = 0; branchIndex < dedupedBranches; branchIndex++) {
        dedupSubqueryBranch(distinctSet, branchIndex, branchResults);
    }

    typeSubqueryBranchResults(branchResults);

    _opBuilder.setInsertionPointToEnd(bodyBlock);

    mlir::db::Union unionOp = _opBuilder.create<mlir::db::Union>(_opBuilder.getUnknownLoc(),
                                                                 branchResults._resultTypes,
                                                                 branchCount);
    const mlir::MutableArrayRef<mlir::Region> unionBranches = unionOp.getBranches();

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        unionBranches[branchIndex].takeBody(*branchResults._regions[branchIndex]);
    }

    publishSubqueryBranchResults(unionOp.getResults(), branchResults, yielded, returnedEntities);
}

void DBProgramGenerator::generateSubqueryConditional(const ConditionalQuery* conditional,
                                                     const BodyScopes& scopes,
                                                     bool answersRows,
                                                     llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                                     CarriedEntities& returnedEntities) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const bodyBlock = _opBuilder.getInsertionBlock();
    const ConditionalQuery::Branches& branches = conditional->branches();
    const BranchScope& predicateScope = findBranchScope(scopes, conditional);

    std::vector<std::unique_ptr<mlir::Region>> conditionRegions;
    SubqueryBranchResults branchResults;

    // A condition's region does not dominate its branch's, so the branch rebinds the scope
    // rather than read a value the condition bound
    for (const ConditionalQuery::Branch& branch : branches) {
        if (const Expr* predicate = branch._condition) {
            rebindSubqueryBranchScope(predicateScope);
            generateConditionRegion(predicate, bodyBlock, conditionRegions);
        }

        generateSubqueryBranch(branch._query,
                               scopes,
                               answersRows,
                               openSubqueryBranch(branchResults),
                               bodyBlock,
                               branchResults);
    }

    yieldEveryPendingMask(branchResults);
    typeSubqueryBranchResults(branchResults);

    _opBuilder.setInsertionPointToEnd(bodyBlock);

    const size_t regionCount = conditionRegions.size() + branches.size();
    mlir::db::Conditional conditionalOp = _opBuilder.create<mlir::db::Conditional>(loc,
                                                                                   branchResults._resultTypes,
                                                                                   regionCount);
    const mlir::MutableArrayRef<mlir::Region> regions = conditionalOp.getBranches();

    size_t regionIndex = 0;
    for (size_t branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
        if (branchIndex < conditionRegions.size()) {
            regions[regionIndex].takeBody(*conditionRegions[branchIndex]);
            regionIndex++;
        }

        regions[regionIndex].takeBody(*branchResults._regions[branchIndex]);
        regionIndex++;
    }

    publishSubqueryBranchResults(conditionalOp.getResults(), branchResults, yielded, returnedEntities);
}

void DBProgramGenerator::generateConditionRegion(const Expr* predicate,
                                                 mlir::Block* bodyBlock,
                                                 std::vector<std::unique_ptr<mlir::Region>>& regions) {
    std::unique_ptr<mlir::Region>& region = regions.emplace_back(std::make_unique<mlir::Region>());
    mlir::Block* const block = new mlir::Block(); // Region destructor frees it
    region->push_back(block);

    _unionBranchRowBlocks[block] = findRowBlock(bodyBlock);

    _opBuilder.setInsertionPointToStart(block);
    translateExpr(predicate);

    const mlir::Value condition = _part._exprMap.at(predicate);
    _opBuilder.create<mlir::db::Yield>(_opBuilder.getUnknownLoc(), mlir::ValueRange {condition});

    _unionBranchRowBlocks.erase(block);
}

mlir::Block* DBProgramGenerator::openSubqueryBranch(SubqueryBranchResults& branchResults) {
    std::unique_ptr<mlir::Region>& region = branchResults._regions.emplace_back(std::make_unique<mlir::Region>());
    mlir::Block* const block = new mlir::Block(); // Region destructor frees it
    region->push_back(block);

    return block;
}

// A branch that is a UNION or a WHEN of its own nests the op holding its branches in the
// branch's region, and those branches start from the rows the outermost one does
void DBProgramGenerator::generateSubqueryBranch(const RegularQuery* query,
                                                const BodyScopes& scopes,
                                                bool answersRows,
                                                mlir::Block* block,
                                                mlir::Block* rowBlock,
                                                SubqueryBranchResults& branchResults) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    _unionBranchRowBlocks[block] = findRowBlock(rowBlock);

    _opBuilder.setInsertionPointToEnd(block);

    llvm::SmallVector<PublishedColumn>& branchColumns = branchResults._branchColumns.emplace_back();
    std::vector<std::optional<PartScope::WrittenEntity>>& writtenColumns = branchResults._writtenColumns.emplace_back();

    if (query->getKind() == QueryCommand::Kind::SINGLE_PART_QUERY) {
        const SinglePartQuery* single = static_cast<const SinglePartQuery*>(query);

        rebindSubqueryBranchScope(findBranchScope(scopes, single));
        generateQueryParts(single);

        if (const ReturnStmt* returnStmt = single->getReturnStmt()) {
            publishProjection(returnStmt->getProjection(), nullptr, true);
            collectPublishedColumns(branchColumns);
        } else if (answersRows) {
            llvm::SmallVector<mlir::Value> answer {constantBool(true)};
            broadcastConstantProjection(answer);

            branchColumns.push_back({nullptr, std::string(ANSWER_COLUMN_NAME), answer.front()});
        }

        for (const PublishedColumn& column : branchColumns) {
            std::optional<PartScope::WrittenEntity>& written = writtenColumns.emplace_back();
            if (const PartScope::WrittenEntity* entity = findWrittenEntity(column._decl)) {
                written = *entity;
            }
        }
    } else {
        CarriedEntities returnedEntities;
        generateSubqueryBody(query, scopes, answersRows, branchColumns, returnedEntities);

        for (const PublishedColumn& column : branchColumns) {
            std::optional<PartScope::WrittenEntity>& written = writtenColumns.emplace_back();

            const auto returnedIt = std::ranges::find(returnedEntities, column._decl, &CarriedEntities::value_type::first);
            if (returnedIt != returnedEntities.end()) {
                written = returnedIt->second;
            }
        }
    }

    llvm::SmallVector<mlir::Value> projected;
    for (const PublishedColumn& column : branchColumns) {
        projected.push_back(column._column);
    }

    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {projected});
    _unionBranchRowBlocks.erase(block);
}

mlir::Block* DBProgramGenerator::findRowBlock(mlir::Block* block) const {
    const auto rowBlockIt = _unionBranchRowBlocks.find(block);

    return rowBlockIt != _unionBranchRowBlocks.end() ? rowBlockIt->second : block;
}

void DBProgramGenerator::dedupSubqueryBranch(mlir::Value distinctSet,
                                             size_t branchIndex,
                                             SubqueryBranchResults& branchResults) {
    mlir::db::Yield yield = mlir::cast<mlir::db::Yield>(branchResults._regions[branchIndex]->front().back());
    _opBuilder.setInsertionPoint(yield);

    const mlir::OperandRange columns = yield.getColumns();
    llvm::SmallVector<mlir::Value> projected(columns.begin(), columns.end());
    dedupUnionBranch(distinctSet, projected);

    yield->setOperands(projected);

    llvm::SmallVector<PublishedColumn>& branchColumns = branchResults._branchColumns[branchIndex];
    for (size_t columnIndex = 0; columnIndex < projected.size(); columnIndex++) {
        branchColumns[columnIndex]._column = projected[columnIndex];
    }
}

void DBProgramGenerator::yieldEveryPendingMask(SubqueryBranchResults& branchResults) {
    std::vector<llvm::SmallVector<PublishedColumn>>& allBranchColumns = branchResults._branchColumns;
    std::vector<std::vector<std::optional<PartScope::WrittenEntity>>>& allWrittenColumns = branchResults._writtenColumns;
    const size_t branchCount = branchResults._regions.size();

    // A column one branch created and another did not is told apart row by row: every
    // row of a branch that created it is pending
    std::vector<std::string> partlyWritten;
    for (const PublishedColumn& column : allBranchColumns.front()) {
        size_t writtenCount = 0;
        for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
            const llvm::SmallVector<PublishedColumn>& branchColumns = allBranchColumns[branchIndex];
            const auto columnIt = std::ranges::find(branchColumns, column._name, &PublishedColumn::_name);

            const bool written = columnIt != branchColumns.end()
                              && allWrittenColumns[branchIndex][columnIt - branchColumns.begin()];
            if (written) {
                writtenCount++;
            }
        }

        if (writtenCount != 0 && writtenCount != branchCount) {
            partlyWritten.push_back(column._name);
        }
    }

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        llvm::SmallVector<PublishedColumn>& branchColumns = allBranchColumns[branchIndex];
        std::vector<std::optional<PartScope::WrittenEntity>>& writtenColumns = allWrittenColumns[branchIndex];

        _opBuilder.setInsertionPoint(&branchResults._regions[branchIndex]->front().back());

        for (const std::string& name : partlyWritten) {
            const auto columnIt = std::ranges::find(branchColumns, name, &PublishedColumn::_name);
            bioassert(columnIt != branchColumns.end(), "Column '{}' missing from a subquery branch", name);

            std::optional<PartScope::WrittenEntity>& written = writtenColumns[columnIt - branchColumns.begin()];
            if (!written) {
                continue;
            }

            written.reset();

            branchColumns.push_back({nullptr, pendingMaskName(name), constantBool(true)});
            writtenColumns.emplace_back();
        }
    }

    std::vector<std::string> names;
    for (const llvm::SmallVector<PublishedColumn>& branchColumns : allBranchColumns) {
        for (const PublishedColumn& column : branchColumns) {
            if (!llvm::is_contained(names, column._name)) {
                names.push_back(column._name);
            }
        }
    }

    std::ranges::sort(names);

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        llvm::SmallVector<PublishedColumn>& branchColumns = allBranchColumns[branchIndex];
        std::vector<std::optional<PartScope::WrittenEntity>>& writtenColumns = allWrittenColumns[branchIndex];

        mlir::db::Yield yield = mlir::cast<mlir::db::Yield>(branchResults._regions[branchIndex]->front().back());
        _opBuilder.setInsertionPoint(yield);

        llvm::SmallVector<PublishedColumn> alignedColumns;
        std::vector<std::optional<PartScope::WrittenEntity>> alignedWritten;

        for (const std::string& name : names) {
            const auto columnIt = std::ranges::find(branchColumns, name, &PublishedColumn::_name);
            if (columnIt != branchColumns.end()) {
                alignedColumns.push_back(*columnIt);
                alignedWritten.push_back(writtenColumns[columnIt - branchColumns.begin()]);
                continue;
            }

            bioassert(name.starts_with(pendingMaskPrefix), "Column '{}' missing from a subquery branch", name);

            // None of the rows of a branch that did not write the entity is pending
            alignedColumns.push_back({nullptr, name, constantBool(false)});
            alignedWritten.emplace_back();
        }

        llvm::SmallVector<mlir::Value> projected;
        for (const PublishedColumn& column : alignedColumns) {
            projected.push_back(column._column);
        }

        yield->setOperands(projected);

        branchColumns = std::move(alignedColumns);
        writtenColumns = std::move(alignedWritten);
    }
}

void DBProgramGenerator::typeSubqueryBranchResults(SubqueryBranchResults& branchResults) {
    llvm::SmallVector<mlir::Type>& resultTypes = branchResults._resultTypes;
    for (const PublishedColumn& column : branchResults._branchColumns.front()) {
        resultTypes.push_back(column._column.getType());
    }

    // A null or a property names no element type until lowering resolves it, so the
    // first branch that names one types the result
    for (const llvm::SmallVector<PublishedColumn>& branchColumns : branchResults._branchColumns) {
        for (size_t columnIndex = 0; columnIndex < branchColumns.size(); columnIndex++) {
            const mlir::db::ColumnType resultType = mlir::cast<mlir::db::ColumnType>(resultTypes[columnIndex]);
            const mlir::Type branchType = branchColumns[columnIndex]._column.getType();
            const bool resultNamesNoType = mlir::isa<mlir::NoneType>(resultType.getType()) || isUntypedNullColumnType(resultType);

            if (resultNamesNoType && !isUntypedNullColumnType(branchType)) {
                resultTypes[columnIndex] = branchType;
            }
        }
    }
}

void DBProgramGenerator::publishSubqueryBranchResults(mlir::ResultRange results,
                                                      const SubqueryBranchResults& branchResults,
                                                      llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                                      CarriedEntities& returnedEntities) {
    const llvm::SmallVector<PublishedColumn>& resultColumns = branchResults._branchColumns.front();
    const std::vector<std::vector<std::optional<PartScope::WrittenEntity>>>& writtenColumns = branchResults._writtenColumns;

    for (size_t columnIndex = 0; columnIndex < resultColumns.size(); columnIndex++) {
        const PublishedColumn& column = resultColumns[columnIndex];
        yielded.push_back({column._decl, column._name, results[columnIndex]});

        // A read of what the query wrote goes to the write buffer for every row of the
        // column or for none, so every branch has to have written it
        std::optional<PartScope::WrittenEntity> returned = writtenColumns.front()[columnIndex];

        for (const std::vector<std::optional<PartScope::WrittenEntity>>& branchWritten : writtenColumns) {
            const std::optional<PartScope::WrittenEntity>& written = branchWritten[columnIndex];

            if (written.has_value() != returned.has_value()) {
                throwError(fmt::format("Column '{}' of a CALL subquery holds what one branch "
                                       "created and another did not, which is not supported",
                                       column._name));
            }

            const bool sameTypes = !written || *written == *returned;
            if (!sameTypes) {
                returned->_labels.clear();
                returned->_edgeType.clear();
            }
        }

        if (returned) {
            returnedEntities.emplace_back(column._decl, *returned);
        }
    }
}

mlir::Value DBProgramGenerator::getOrTranslateExprColumn(const VariableColumnMap& variableColumns,
                                                         const Expr* expr) {
    const mlir::Value column = getOrTranslateBoundColumn(variableColumns, expr);

    return expr->getKind() == Expr::Kind::SYMBOL ? readWalkEntities(expr, column) : column;
}

mlir::Value DBProgramGenerator::getOrTranslateBoundColumn(const VariableColumnMap& variableColumns,
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

    translateExprOverRows(expr);
    return _part._exprMap.at(expr);
}

mlir::Value DBProgramGenerator::getOrTranslateExprColumn(const Expr* expr) {
    const mlir::Value column = getOrTranslateBoundColumn(expr);

    return expr->getKind() == Expr::Kind::SYMBOL ? readWalkEntities(expr, column) : column;
}

// The column as bound, which for a group variable is its walk's handle rather than its list.
// Without a map, the one name the expression could name is looked up over the bindings
// rather than every one of them being gathered to answer for it
mlir::Value DBProgramGenerator::getOrTranslateBoundColumn(const Expr* expr) {
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

        case Expr::Kind::PROPERTY_LOOKUP: {
            const PropertyLookupExpr* lookupExpr = static_cast<const PropertyLookupExpr*>(expr);
            _part._exprMap[expr] = translatePropertyLookupExpr(lookupExpr);
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

        case Expr::Kind::LOGICAL: {
            const LogicalExpr* logicalExpr = static_cast<const LogicalExpr*>(expr);
            translateLogicalExpr(expr, logicalExpr);
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

            if (projectedIt != _part._projectedColumns.end()) {
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
            if (namesACSVRow) {
                throwCSVRowRead(expr, varName);
            }

            bioassert(bound, "Symbol refers to unknown variable: {}", varName);

            _part._exprMap[expr] = readWalkEntities(expr, _part._exprMap.at(expr));
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
            translateIndexExpr(expr, indexExpr);
        }
        break;

        case Expr::Kind::LIST_SLICE: {
            const ListSliceExpr* slice = static_cast<const ListSliceExpr*>(expr);
            translateListSliceExpr(expr, slice);
        }
        break;

        case Expr::Kind::ENTITY_TYPES: {
            const EntityTypeExpr* typeExpr = static_cast<const EntityTypeExpr*>(expr);
            _part._exprMap[expr] = translateEntityTypeExpr(typeExpr);
        }
        break;

        case Expr::Kind::CASE: {
            const CaseExpr* caseExpr = static_cast<const CaseExpr*>(expr);
            translateCaseExpr(expr, caseExpr);
        }
        break;

        case Expr::Kind::EXISTS: {
            const ExistsExpr* existsExpr = static_cast<const ExistsExpr*>(expr);
            translateExistsExpr(expr, existsExpr);
        }
        break;

        case Expr::Kind::COUNT_SUBQUERY: {
            const CountSubqueryExpr* countExpr = static_cast<const CountSubqueryExpr*>(expr);
            translateCountSubqueryExpr(expr, countExpr);
        }
        break;
        case Expr::Kind::COLLECT_SUBQUERY: {
            const CollectSubqueryExpr* collectExpr = static_cast<const CollectSubqueryExpr*>(expr);
            translateCollectSubqueryExpr(expr, collectExpr);
        }
        break;

        case Expr::Kind::LIST_COMPREHENSION: {
            const ListComprehensionExpr* comprehension = static_cast<const ListComprehensionExpr*>(expr);
            translateListComprehensionExpr(expr, comprehension);
        }
        break;

        case Expr::Kind::PATTERN_COMPREHENSION: {
            const PatternComprehensionExpr* comprehension =
                static_cast<const PatternComprehensionExpr*>(expr);
            translatePatternComprehensionExpr(expr, comprehension);
        }
        break;

        case Expr::Kind::LIST_PREDICATE: {
            const ListPredicateExpr* predicate = static_cast<const ListPredicateExpr*>(expr);
            translateListPredicateExpr(expr, predicate);
        }
        break;

        case Expr::Kind::REDUCE: {
            const ReduceExpr* reduce = static_cast<const ReduceExpr*>(expr);
            translateReduceExpr(expr, reduce);
        }
        break;

        case Expr::Kind::ALL_REDUCE: {
            const AllReduceExpr* allReduce = static_cast<const AllReduceExpr*>(expr);
            translateAllReduceExpr(expr, allReduce);
        }
        break;

        case Expr::Kind::MAP_PROJECTION: {
            const MapProjectionExpr* projection = static_cast<const MapProjectionExpr*>(expr);
            translateMapProjectionExpr(expr, projection);
        }
        break;

        case Expr::Kind::LIST:
            throwError(fmt::format("Unsupported expression: {}",
                                   ExprKindDescription::value(kind)),
                       expr);
        break;

        case Expr::Kind::PARAMETER:
            _part._exprMap[expr] = translateParameterExpr(static_cast<const ParameterExpr*>(expr));
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
            if (isUntypedNullColumn(operandVal)) {
                _part._exprMap[expr] = nullConstantColumn();
                break;
            }

            const mlir::db::ColumnType boolType =
                allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
            auto notOp = _opBuilder.create<mlir::db::NotOp>(loc, boolType, operandVal);
            const mlir::Value result = notOp.getResult();
            _part._exprMap[expr] = result;
        }
        break;

        case UnaryOperator::Minus: {
            // Negating an unknown value is unknown: the null the operand is stands as the
            // negation
            if (expr->getType() == EvaluatedType::Null) {
                _part._exprMap[expr] = nullConstantColumn();
                break;
            }

            _part._exprMap[expr] = _opBuilder.create<mlir::db::Negate>(loc, operandVal).getResult();
        }
        break;

        case UnaryOperator::Plus: {
            const EvaluatedType operandType = subExpr->getType();
            if (operandType == EvaluatedType::Null) {
                _part._exprMap[expr] = nullConstantColumn();
                break;
            } else if (operandType != EvaluatedType::Any) {
                _part._exprMap[expr] = operandVal;
                break;
            }

            _part._exprMap[expr] = _opBuilder.create<mlir::db::UnaryPlus>(loc, operandVal).getResult();
        }
        break;

        case UnaryOperator::_SIZE:
            throwError("Unknown unary operator.", expr);
        break;
    }
}

void DBProgramGenerator::translateListComprehensionExpr(const Expr* expr,
                                                        const ListComprehensionExpr* comprehension) {
    const Expr* const sourceExpr = comprehension->getSource();
    const mlir::Value source = getOrTranslateExprColumn(sourceExpr);

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    auto comprehensionOp = _opBuilder.create<mlir::db::ListComprehension>(_opBuilder.getUnknownLoc(),
                                                                          noneType,
                                                                          source,
                                                                          carrySet._columns);

    generateElementRegion(comprehensionOp.getBody(),
                          comprehension,
                          carrySet,
                          /*cutsElements=*/true,
                          comprehension->getProjection());

    dropUnreadCarriedColumns(comprehensionOp, comprehensionOp.getColumnsToFilter());

    _part._exprMap[expr] = comprehensionOp.getResult();
}

void DBProgramGenerator::translateListPredicateExpr(const Expr* expr, const ListPredicateExpr* predicate) {
    const ListComprehensionExpr* comprehension = predicate->getComprehension();
    const Expr* const sourceExpr = comprehension->getSource();
    const mlir::Value source = getOrTranslateExprColumn(sourceExpr);

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const mlir::storage::ListPredicateKind kind = listPredicateKind(predicate->getQuantifier());
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    auto predicateOp = _opBuilder.create<mlir::db::ListPredicate>(_opBuilder.getUnknownLoc(),
                                                                  boolType,
                                                                  source,
                                                                  carrySet._columns,
                                                                  kind);

    generateElementRegion(predicateOp.getBody(),
                          comprehension,
                          carrySet,
                          /*cutsElements=*/false,
                          comprehension->getPredicate());

    dropUnreadCarriedColumns(predicateOp, predicateOp.getColumnsToFilter());

    _part._exprMap[expr] = predicateOp.getResult();
}

void DBProgramGenerator::translateReduceExpr(const Expr* expr, const ReduceExpr* reduce) {
    const mlir::Value initialValue = getOrTranslateExprColumn(reduce->getInitialValue());
    const mlir::Value source = getOrTranslateExprColumn(reduce->getSource());

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    auto reduceOp = _opBuilder.create<mlir::db::Reduce>(loc,
                                                        noneType,
                                                        source,
                                                        initialValue,
                                                        carrySet._columns);

    generateFoldRegion(reduceOp.getBody(), carrySet, reduce->getItemDecl(), reduce->getAccumulatorDecl(), [&]() {
        const mlir::Value value = getOrTranslateExprColumn(reduce->getExpression());
        _opBuilder.create<mlir::db::ReduceYield>(loc, value);
    });

    dropUnreadCarriedColumns(reduceOp, reduceOp.getColumnsToFilter());

    _part._exprMap[expr] = reduceOp.getResult();
}

void DBProgramGenerator::translateAllReduceExpr(const Expr* expr, const AllReduceExpr* allReduce) {
    const mlir::Value initialValue = getOrTranslateExprColumn(allReduce->getInitialValue());
    const mlir::Value source = getOrTranslateExprColumn(allReduce->getSource());

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    auto allReduceOp = _opBuilder.create<mlir::db::AllReduce>(loc,
                                                              boolType,
                                                              source,
                                                              initialValue,
                                                              carrySet._columns);

    const VarDecl* const itemDecl = allReduce->getItemDecl();
    const VarDecl* const accumulatorDecl = allReduce->getAccumulatorDecl();

    generateFoldRegion(allReduceOp.getBody(), carrySet, itemDecl, accumulatorDecl, [&]() {
        const mlir::Value value = getOrTranslateExprColumn(allReduce->getExpression());
        _opBuilder.create<mlir::db::ReduceYield>(loc, value);
    });

    generateFoldRegion(allReduceOp.getPredicate(), carrySet, itemDecl, accumulatorDecl, [&]() {
        const mlir::Value holds = getOrTranslateExprColumn(allReduce->getPredicate());
        _opBuilder.create<mlir::db::AllReduceYield>(loc, holds);
    });

    dropUnreadCarriedColumns(allReduceOp, allReduceOp.getColumnsToFilter());

    _part._exprMap[expr] = allReduceOp.getResult();
}

void DBProgramGenerator::translateMapProjectionExpr(const Expr* expr, const MapProjectionExpr* projection) {
    if (projection->getType() == EvaluatedType::Null) {
        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const llvm::SmallVector<mlir::Value> carried(carrySet._columns.begin(), carrySet._columns.end());

    auto caseOp = _opBuilder.create<mlir::db::LazyCase>(loc, noneType, carried, 2u);
    const mlir::MutableArrayRef<mlir::Region> regions = caseOp.getBranches();

    generateCaseRegion(regions[0], carried, carrySet, mlir::Value {}, [&](mlir::Value) {
        const mlir::Value base = getOrTranslateExprColumn(projection->getBase());
        return _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, base).getResult();
    });

    generateCaseRegion(regions[1], carried, carrySet, mlir::Value {}, [&](mlir::Value) {
        return projectedMap(projection);
    });

    _part._exprMap[expr] = selectionResult(caseOp);
}

mlir::Value DBProgramGenerator::projectedMap(const MapProjectionExpr* projection) {
    bool readsEveryProperty = false;
    llvm::SmallVector<std::pair<std::string_view, const Expr*>> entries;

    for (const MapProjectionExpr::Item& item : projection->getItems()) {
        if (item._kind == MapProjectionExpr::ItemKind::AllProperties) {
            readsEveryProperty = true;
        } else if (!item._shadowed) {
            entries.emplace_back(item._key->getName(), item._value);
        }
    }

    if (!readsEveryProperty) {
        return emitMakeMap(entries);
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Value base = getOrTranslateExprColumn(projection->getBase());

    const mlir::Value properties = _opBuilder.create<mlir::db::PropertyMap>(loc, base).getResult();
    if (entries.empty()) {
        return properties;
    }

    const mlir::Value written = emitMakeMap(entries);

    return _opBuilder.create<mlir::db::MergeMaps>(loc, noneType, mlir::ValueRange {properties, written}).getResult();
}

void DBProgramGenerator::generateFoldRegion(mlir::Region& region,
                                            const CarrySet& carrySet,
                                            const VarDecl* itemDecl,
                                            const VarDecl* accumulatorDecl,
                                            FoldBodyGenerator generateBody) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    llvm::SmallVector<mlir::Type> argumentTypes {noneType, noneType};
    llvm::SmallVector<mlir::Location> argumentLocations {loc, loc};

    for (const mlir::Value column : carrySet._columns) {
        argumentTypes.push_back(column.getType());
        argumentLocations.push_back(loc);
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const bodyBlock = _opBuilder.createBlock(&region, {}, argumentTypes, argumentLocations);

    const VariableIdentityMap outerVarMap = _part._varMap;
    const EdgeTypeColumnMap outerEdgeTypeMap = _part._edgeTypeMap;
    const std::vector<YieldedColumn> outerYieldedColumns = _part._yieldedColumns;
    const ElementColumnMap outerComprehensionElements = _part._comprehensionElements;
    const PartScope::CreatedEntityMap outerCreatedEntities = _part._createdEntities;
    const ExprValueMap outerExprMap = _part._exprMap;
    const ProjectedColumnMap outerProjectedColumns = _part._projectedColumns;
    const PartScope::NamedPathMap outerNamedPaths = _part._namedPaths;

    const mlir::Block::BlockArgListType carriedArguments = bodyBlock->getArguments().drop_front(2);

    _part._exprMap.clear();
    rebindCarrySet(carriedArguments, /*firstColumn=*/0, carrySet);
    rebindProjectedColumns(carrySet._columns, carriedArguments);
    _part._comprehensionElements[itemDecl] = bodyBlock->getArgument(0);
    _part._comprehensionElements[accumulatorDecl] = bodyBlock->getArgument(1);

    generateBody();

    _part._varMap = outerVarMap;
    _part._edgeTypeMap = outerEdgeTypeMap;
    _part._yieldedColumns = outerYieldedColumns;
    _part._comprehensionElements = outerComprehensionElements;
    _part._createdEntities = outerCreatedEntities;
    _part._exprMap = outerExprMap;
    _part._projectedColumns = outerProjectedColumns;
    _part._namedPaths = outerNamedPaths;
}

void DBProgramGenerator::generateElementRegion(mlir::Region& region,
                                               const ListComprehensionExpr* comprehension,
                                               const CarrySet& carrySet,
                                               bool cutsElements,
                                               const Expr* valueExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::db::ColumnType rowTagType =
        allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false));

    llvm::SmallVector<mlir::Type> argumentTypes {noneType, rowTagType};
    llvm::SmallVector<mlir::Location> argumentLocations {loc, loc};

    for (const mlir::Value column : carrySet._columns) {
        argumentTypes.push_back(column.getType());
        argumentLocations.push_back(loc);
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const bodyBlock = _opBuilder.createBlock(&region, {}, argumentTypes, argumentLocations);

    const VariableIdentityMap outerVarMap = _part._varMap;
    const EdgeTypeColumnMap outerEdgeTypeMap = _part._edgeTypeMap;
    const std::vector<YieldedColumn> outerYieldedColumns = _part._yieldedColumns;
    const ElementColumnMap outerComprehensionElements = _part._comprehensionElements;
    const PartScope::CreatedEntityMap outerCreatedEntities = _part._createdEntities;
    const ExprValueMap outerExprMap = _part._exprMap;
    const ProjectedColumnMap outerProjectedColumns = _part._projectedColumns;
    const PartScope::NamedPathMap outerNamedPaths = _part._namedPaths;

    const VarDecl* const itemDecl = comprehension->getDecl();

    _part._exprMap.clear();
    rebindCarrySet(bodyBlock->getArguments().drop_front(2), /*firstColumn=*/0, carrySet);
    rebindProjectedColumns(carrySet._columns, bodyBlock->getArguments().drop_front(2));
    _part._comprehensionElements[itemDecl] = bodyBlock->getArgument(0);

    mlir::Value rowTags = bodyBlock->getArgument(1);

    // The WHERE cuts the elements themselves rather than masking what they contribute
    mlir::db::FilterOp elementFilter;
    const Expr* const predicateExpr = comprehension->getPredicate();
    if (cutsElements && predicateExpr) {
        const mlir::Value predicate = getOrTranslateExprColumn(predicateExpr);

        const llvm::SmallVector<mlir::Value> filtered(bodyBlock->args_begin(), bodyBlock->args_end());

        llvm::SmallVector<mlir::Type> filteredTypes;
        for (const mlir::Value column : filtered) {
            filteredTypes.push_back(column.getType());
        }

        elementFilter = _opBuilder.create<mlir::db::FilterOp>(loc, filteredTypes, predicate, filtered);

        _part._comprehensionElements[itemDecl] = elementFilter.getResult(0);
        rowTags = elementFilter.getResult(1);

        _part._exprMap.clear();
        rebindCarrySet(elementFilter.getResults(), /*firstColumn=*/2, carrySet);
        rebindProjectedColumns(filtered, elementFilter.getResults());
    }

    const mlir::Value value = valueExpr ? getOrTranslateExprColumn(valueExpr)
                                       : _part._comprehensionElements[itemDecl];

    _opBuilder.create<mlir::db::ComprehensionYield>(loc, rowTags, value);

    _part._varMap = outerVarMap;
    _part._edgeTypeMap = outerEdgeTypeMap;
    _part._yieldedColumns = outerYieldedColumns;
    _part._comprehensionElements = outerComprehensionElements;
    _part._createdEntities = outerCreatedEntities;
    _part._exprMap = outerExprMap;
    _part._projectedColumns = outerProjectedColumns;
    _part._namedPaths = outerNamedPaths;

    if (elementFilter) {
        dropUnreadFilteredColumns(elementFilter);
    }
}

void DBProgramGenerator::dropUnreadCarriedColumns(mlir::Operation* op, mlir::OperandRange carried) {
    if (carried.empty()) {
        return;
    }

    const size_t carriedCount = carried.size();
    const size_t operandOffset = carried.getBeginOperandIndex();

    llvm::BitVector unread(carriedCount, true);
    for (mlir::Region& region : op->getRegions()) {
        const mlir::Block::BlockArgListType carriedArguments = region.front().getArguments().take_back(carriedCount);

        for (size_t carriedIndex = 0; carriedIndex < carriedCount; carriedIndex++) {
            if (!carriedArguments[carriedIndex].use_empty()) {
                unread.reset(carriedIndex);
            }
        }
    }

    if (unread.none()) {
        return;
    }

    for (mlir::Region& region : op->getRegions()) {
        mlir::Block& block = region.front();
        const size_t argumentOffset = block.getNumArguments() - carriedCount;

        llvm::BitVector erasedArguments(block.getNumArguments());
        for (const unsigned carriedIndex : unread.set_bits()) {
            _constantColumns.erase(block.getArgument(argumentOffset + carriedIndex));
            erasedArguments.set(argumentOffset + carriedIndex);
        }

        block.eraseArguments(erasedArguments);
    }

    llvm::BitVector erasedOperands(op->getNumOperands());
    for (const unsigned carriedIndex : unread.set_bits()) {
        erasedOperands.set(operandOffset + carriedIndex);
    }

    op->eraseOperands(erasedOperands);
}

void DBProgramGenerator::dropUnreadFilteredColumns(mlir::db::FilterOp filter) {
    const mlir::OperandRange columns = filter.getColumnsToFilter();
    const mlir::ResultRange results = filter.getResults();

    llvm::SmallVector<mlir::Value> keptColumns;
    llvm::SmallVector<mlir::Type> keptTypes;
    llvm::SmallVector<mlir::Value> keptResults;

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value result = results[columnIndex];
        if (!result.use_empty()) {
            keptColumns.push_back(columns[columnIndex]);
            keptTypes.push_back(result.getType());
            keptResults.push_back(result);
        }
    }

    if (keptColumns.size() == columns.size()) {
        return;
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    _opBuilder.setInsertionPoint(filter);

    auto trimmed = _opBuilder.create<mlir::db::FilterOp>(filter.getLoc(), keptTypes, filter.getMask(), keptColumns);

    for (size_t keptIndex = 0; keptIndex < keptResults.size(); keptIndex++) {
        keptResults[keptIndex].replaceAllUsesWith(trimmed.getResult(keptIndex));
    }

    forgetErasedValues(filter, _constantColumns);
    filter.erase();
}

void DBProgramGenerator::translatePatternComprehensionExpr(const Expr* expr,
                                                           const PatternComprehensionExpr* comprehension) {
    MatchStmt* const match = comprehension->getMatch();

    throwOnMatchOverWrittenEntity(match, "A pattern comprehension");

    // The lists stand beside the rows in flight, so every column of those rows is handed
    // to the op: the pattern joins onto the ones it names and carries the rest along
    CarrySet carrySet;
    collectCarrySet(carrySet);

    if (_part._orderingProjection) {
        carryProjectedColumns(carrySet);
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    auto comprehensionOp = _opBuilder.create<mlir::db::PatternComprehension>(loc,
                                                                             noneType,
                                                                             carrySet._columns);

    llvm::SmallVector<mlir::Type> argumentTypes;
    llvm::SmallVector<mlir::Location> argumentLocations;

    for (const mlir::Value column : carrySet._columns) {
        argumentTypes.push_back(column.getType());
        argumentLocations.push_back(loc);
    }

    // No column in flight means no input row to tag: the one list the comprehension builds
    // covers the single empty row the query starts from
    const bool tagsRows = !carrySet._columns.empty();

    if (tagsRows) {
        argumentTypes.push_back(allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false)));
        argumentLocations.push_back(loc);
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const patternBlock = _opBuilder.createBlock(&comprehensionOp.getPattern(),
                                                             {},
                                                             argumentTypes,
                                                             argumentLocations);

    const PartScope outerPart = _part;

    // The pattern reads the rows in flight through the block arguments, so the scope is
    // moved onto them before it is handed over
    rebindCarrySet(patternBlock->getArguments(), /*firstColumn=*/0, carrySet);

    llvm::SmallVector<PublishedColumn> patternScope;
    collectPublishedColumns(patternScope);

    if (_part._orderingProjection) {
        rebindProjectedColumns(carrySet._columns, patternBlock->getArguments());
        publishProjectedAliases(patternScope);
    }

    // The pattern is a MATCH of its own and is walked over a dependency graph of its own;
    // the one the query is being generated from is put back once it has been.
    VariableDependencyGraph outerVdg = std::move(_vdg);

    rebindScopeKeepingWrittenEntities(patternScope);

    const VariableDependency* tagVariable = nullptr;
    if (tagsRows) {
        tagVariable = _vdg.registerBoundVariable(comprehensionTagName, nullptr);
        const unsigned tagArgument = static_cast<unsigned>(carrySet._columns.size());
        registerValue(tagVariable, patternBlock->getArgument(tagArgument));
    }

    Stmt* const patternStatements[] = {match};
    bindCheckedCasts(patternStatements);
    generatePart(patternStatements);

    const mlir::Value value = getOrTranslateExprColumn(comprehension->getProjection());
    const mlir::Value rowTags = tagsRows ? findVarOrThrow(_part._varMap, tagVariable) : mlir::Value {};

    _opBuilder.create<mlir::db::ComprehensionYield>(loc, rowTags, value);

    _part = outerPart;
    _vdg = std::move(outerVdg);

    _part._exprMap[expr] = comprehensionOp.getResult();
}

// A union holds a row exactly when one of its branches does, so each branch is an EXISTS
// of its own and the answer is their OR
void DBProgramGenerator::translateExistsExpr(const Expr* expr, const ExistsExpr* existsExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    llvm::SmallVector<const RegularQuery*> branches;
    collectUnionBranches(existsExpr->getBody(), branches);

    mlir::Value exists;
    for (const RegularQuery* branch : branches) {
        const mlir::Value branchExists =
            generateSubqueryExpression<mlir::db::ExistsSubquery, mlir::db::ExistsYield>(branch,
                                                                                        boolType,
                                                                                        existsTagName);

        if (exists) {
            exists = _opBuilder.create<mlir::db::OrOp>(loc, boolType, exists, branchExists).getResult();
        } else {
            exists = branchExists;
        }
    }

    _part._exprMap[expr] = exists;
}

void DBProgramGenerator::translateCountSubqueryExpr(const Expr* expr, const CountSubqueryExpr* countExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType countType = allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false));

    llvm::SmallVector<const RegularQuery*> branches;
    collectUnionAllBranches(countExpr->getBody(), branches);

    mlir::Value count;
    for (const RegularQuery* branch : branches) {
        const mlir::Value branchCount =
            generateSubqueryExpression<mlir::db::CountSubquery, mlir::db::CountSubqueryYield>(branch,
                                                                                              countType,
                                                                                              countTagName);

        if (count) {
            count = _opBuilder.create<mlir::db::AddOp>(loc, count, branchCount).getResult();
        } else {
            count = branchCount;
        }
    }

    _part._exprMap[expr] = count;
}

// A UNION ALL holds a row's values from every branch, one after the other, so each branch
// is a COLLECT of its own and the lists are concatenated in branch order
void DBProgramGenerator::translateCollectSubqueryExpr(const Expr* expr, const CollectSubqueryExpr* collectExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    llvm::SmallVector<const RegularQuery*> branches;
    collectUnionAllBranches(collectExpr->getBody(), branches);

    mlir::Value lists;
    for (const RegularQuery* branch : branches) {
        const mlir::Value branchLists =
            generateSubqueryExpression<mlir::db::CollectSubquery, mlir::db::CollectSubqueryYield>(branch,
                                                                                                  noneType,
                                                                                                  collectTagName);

        if (lists) {
            lists = _opBuilder.create<mlir::db::ConcatOp>(loc, lists, branchLists).getResult();
        } else {
            lists = branchLists;
        }
    }

    _part._exprMap[expr] = lists;
}

template <typename SubqueryOp, typename YieldOp>
mlir::Value DBProgramGenerator::generateSubqueryExpression(const RegularQuery* body,
                                                           mlir::Type resultType,
                                                           std::string_view tagName) {
    // A UNION dedups the rows of each input row on their own, and a WHEN decides per input
    // row which branch runs, so either body runs one input row at a time
    const bool runsPerRow = body->getKind() != QueryCommand::Kind::SINGLE_PART_QUERY;
    const bool carriesScope = !runsPerRow && subqueryCarriesRows(static_cast<const SinglePartQuery*>(body));

    std::vector<const RegularQuery*> queries;
    RegularQuery::collectQueries(body, queries);

    llvm::SmallVector<PublishedColumn> scopeColumns;
    collectPublishedColumns(scopeColumns);

    // An alias spelling the name of a variable shadows that variable in the ORDER BY
    if (_part._orderingProjection) {
        bool appended = false;

        for (const RegularQuery* query : queries) {
            for (const VarDecl* decl : query->getDeclContext()->decls()) {
                const VarDecl* source = decl->getImportSource();
                const auto projectedIt = _part._projectedColumns.find(source);

                if (!source || projectedIt == _part._projectedColumns.end()) {
                    continue;
                }

                const std::string_view name = decl->getName();
                const PublishedColumn projected {source, std::string(name), projectedIt->second};
                const auto publishedIt = std::ranges::find(scopeColumns, name, &PublishedColumn::_name);

                if (publishedIt != scopeColumns.end()) {
                    *publishedIt = projected;
                } else {
                    scopeColumns.push_back(projected);
                    appended = true;
                }
            }
        }

        if (appended) {
            std::ranges::sort(scopeColumns, {}, &PublishedColumn::_name);
        }
    }

    // A constant holds one value standing for every row rather than rows of its own, so it
    // is no input of the body: it stays bound where it is and the body reads it there
    llvm::SmallVector<PublishedColumn> inputs;
    llvm::SmallVector<PublishedColumn> constants;

    mlir::Value consumedColumn;

    for (const PublishedColumn& column : scopeColumns) {
        const bool consumedByTheReduction = _part._rowReductionOp
                                         && !boundAtOrAfter(column._column, _part._rowReductionOp);

        if (yieldsConstantColumn(column._column, _constantColumns)) {
            constants.push_back(column);
        } else if (!consumedByTheReduction) {
            inputs.push_back(column);
        } else {
            // A group or a distinct row holds no one value of a variable the reduction consumed,
            // and the analyzer turns away a body reading one: a WITH or a RETURN * in the body
            // only carries it
            if (!consumedColumn) {
                consumedColumn = nullConstantColumn();
            }

            constants.push_back({column._decl, column._name, consumedColumn});
        }
    }

    // With no variable holding the rows in flight, the reduction's results alone hold them,
    // and the body then answers for them under no name
    if (inputs.empty() && _part._rowReductionOp) {
        for (const mlir::Value result : _part._rowReductionOp->getResults()) {
            inputs.push_back({nullptr, std::string(), result});
        }
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType tagType = allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false));

    llvm::SmallVector<mlir::Value> inputColumns;
    llvm::SmallVector<mlir::Type> argumentTypes;
    llvm::SmallVector<mlir::Location> argumentLocations;

    for (const PublishedColumn& input : inputs) {
        inputColumns.push_back(input._column);
        argumentTypes.push_back(input._column.getType());
        argumentLocations.push_back(loc);
    }

    // A body that cannot keep its rows paired with the ones it was given carries no tag:
    // it is run one input row at a time, and the row it was handed is the one it answers for
    const bool tagsRows = carriesScope && !inputs.empty();
    if (tagsRows) {
        argumentTypes.push_back(tagType);
        argumentLocations.push_back(loc);
    }

    SubqueryOp subqueryOp = _opBuilder.create<SubqueryOp>(loc, resultType, inputColumns, carriesScope);

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const bodyBlock = _opBuilder.createBlock(&subqueryOp.getBody(),
                                                         {},
                                                         argumentTypes,
                                                         argumentLocations);

    // The body is correlated: each of its queries reads every variable in flight, under the
    // declaration its own context holds for it
    BodyScopes scopes;

    for (const RegularQuery* query : queries) {
        if (query->getKind() == QueryCommand::Kind::UNION_QUERY) {
            continue;
        }

        const DeclContext* bodyContext = query->getDeclContext();
        llvm::SmallVector<PublishedColumn>& scope = scopes[query]._columns;

        // A pattern of the body casting an import binds the import's name to the cast
        const auto importedDecl = [bodyContext](std::string_view name) -> const VarDecl* {
            const VarDecl* named = bodyContext->getDecl(name);
            const VarDecl* decl = named ? named->getCastSourceOrSelf() : nullptr;

            return decl && decl->getImportSource() ? decl : nullptr;
        };

        for (size_t inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
            const PublishedColumn& input = inputs[inputIndex];
            const mlir::Value argument = bodyBlock->getArgument(static_cast<unsigned>(inputIndex));

            if (const VarDecl* correlated = importedDecl(input._name)) {
                scope.push_back({correlated, input._name, argument});
            }

            const std::string_view fieldRow = findCSVFieldRow(input._decl);
            if (!fieldRow.empty() && importedDecl(fieldRow)) {
                scope.push_back({input._decl, input._name, argument});
            }
        }

        if (tagsRows) {
            const mlir::Value tagArgument = bodyBlock->getArgument(static_cast<unsigned>(inputs.size()));
            scope.push_back({nullptr, std::string(tagName), tagArgument});
        }

        for (const PublishedColumn& constant : constants) {
            if (const VarDecl* correlated = importedDecl(constant._name)) {
                scope.push_back({correlated, constant._name, constant._column});
            }
        }
    }

    if (!carriesScope) {
        readArgumentsAsRunConstants(bodyBlock, scopes);
    }

    // What the query holds is set aside while the body builds a scope of its own: the body
    // binds nothing the query goes on to read, the op handing back one column and no other
    PartScope outerPart = std::move(_part);
    VariableDependencyGraph outerGraph = std::move(_vdg);

    std::unordered_set<std::string_view> outerImportedCSVRows;
    std::swap(_importedCSVRows, outerImportedCSVRows);

    const RunConstantMap outerRunConstants = _runConstants;

    llvm::SmallVector<PublishedColumn> held;

    if (runsPerRow) {
        CarriedEntities returnedEntities;
        generateSubqueryBody(body, scopes, !body->isReturning(), held, returnedEntities);
    } else {
        const SinglePartQuery* query = static_cast<const SinglePartQuery*>(body);

        rebindSubqueryBranchScope(findBranchScope(scopes, query));

        generateQueryParts(query);

        // A RETURN answers for no column here, but its cut does: SKIP can empty a body that
        // matched, so the projection is emitted and the rows it leaves are the ones answered for
        if (const ReturnStmt* returnStmt = query->getReturnStmt()) {
            publishProjection(returnStmt->getProjection());
        }

        collectPublishedColumns(held);
    }

    // A tagged body is answered for by its tag alone, which a barrier in the body carries
    // on under its hidden name. A body with no tag is answered for by the rows it holds.
    mlir::Value carriedTag;
    llvm::SmallVector<mlir::Value> heldColumns;

    if (tagsRows) {
        const auto tagIt = std::ranges::find(held, tagName, &PublishedColumn::_name);
        bioassert(tagIt != held.end(), "Row tag lost by a subquery body");

        carriedTag = tagIt->_column;
    } else {
        for (const PublishedColumn& column : held) {
            heldColumns.push_back(column._column);
        }
    }

    if constexpr (std::is_same_v<SubqueryOp, mlir::db::CollectSubquery>) {
        const auto isReturned = [](const PublishedColumn& column) {
            return !isHiddenName(column._name);
        };

        const auto valueIt = std::ranges::find_if(held, isReturned);
        bioassert(valueIt != held.end(), "COLLECT subquery body returns no column");

        _opBuilder.create<YieldOp>(loc, carriedTag, valueIt->_column, heldColumns);
    } else {
        _opBuilder.create<YieldOp>(loc, carriedTag, heldColumns);
    }

    _part = std::move(outerPart);
    _vdg = std::move(outerGraph);

    std::swap(_importedCSVRows, outerImportedCSVRows);
    _runConstants = outerRunConstants;

    return subqueryOp.getResult();
}

void DBProgramGenerator::translateCaseExpr(const Expr* expr, const CaseExpr* caseExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    // The simple form compares its subject against every WHEN value, which is the generic
    // form with that equality as the condition
    const Expr* const subjectExpr = caseExpr->getSubject();
    mlir::Value subject;
    if (subjectExpr) {
        translateExpr(subjectExpr);
        bioassert(_part._exprMap.contains(subjectExpr), "CASE subject with no column.");
        subject = _part._exprMap.at(subjectExpr);
    }

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    llvm::SmallVector<mlir::Value> carried(carrySet._columns.begin(), carrySet._columns.end());
    if (subject && !yieldsConstantColumn(subject, _constantColumns)) {
        carried.push_back(subject);
    }

    const CaseExpr::Branches& branches = caseExpr->getBranches();
    const Expr* const elseExpr = caseExpr->getElseExpr();
    const size_t regionCount = 2 * branches.size() + (elseExpr ? 1 : 0);

    auto caseOp = _opBuilder.create<mlir::db::LazyCase>(loc,
                                                        noneType,
                                                        carried,
                                                        static_cast<unsigned>(regionCount));

    const mlir::MutableArrayRef<mlir::Region> regions = caseOp.getBranches();

    const auto translateValue = [this](const Expr* valueExpr) {
        translateExpr(valueExpr);
        bioassert(_part._exprMap.contains(valueExpr), "CASE branch with no value column.");

        return _part._exprMap.at(valueExpr);
    };

    size_t regionIndex = 0;
    for (const CaseExpr::Branch& branch : branches) {
        generateCaseRegion(regions[regionIndex], carried, carrySet, subject, [&](mlir::Value regionSubject) {
            mlir::Value condition;

            for (const CaseExpr::Test& test : branch._tests) {
                const mlir::Value tested = translateCaseTest(subjectExpr, regionSubject, test);

                if (condition) {
                    condition = _opBuilder.create<mlir::db::OrOp>(loc, boolType, condition, tested).getResult();
                } else {
                    condition = tested;
                }
            }

            return condition;
        });

        generateCaseRegion(regions[regionIndex + 1], carried, carrySet, subject, [&](mlir::Value) {
            return translateValue(branch._then);
        });

        regionIndex += 2;
    }

    if (elseExpr) {
        generateCaseRegion(regions[regionIndex], carried, carrySet, subject, [&](mlir::Value) {
            return translateValue(elseExpr);
        });
    }

    _part._exprMap[expr] = selectionResult(caseOp);
}

void DBProgramGenerator::generateCaseRegion(mlir::Region& region,
                                            llvm::ArrayRef<mlir::Value> carried,
                                            const CarrySet& carrySet,
                                            mlir::Value subject,
                                            CaseRegionGenerator generateValue) {
    llvm::SmallVector<mlir::Type> argumentTypes;
    llvm::SmallVector<mlir::Location> argumentLocations;

    for (const mlir::Value column : carried) {
        argumentTypes.push_back(column.getType());
        argumentLocations.push_back(_opBuilder.getUnknownLoc());
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const block = _opBuilder.createBlock(&region, {}, argumentTypes, argumentLocations);
    const mlir::Block::BlockArgListType arguments = block->getArguments();

    const PartScope outerPart = _part;

    _part._exprMap.clear();
    rebindCarrySet(arguments, /*firstColumn=*/0, carrySet);
    rebindProjectedColumns(carrySet._columns, arguments.take_front(carrySet._columns.size()));

    const bool carriesSubject = carried.size() > carrySet._columns.size();
    const mlir::Value regionSubject = carriesSubject ? mlir::Value(arguments.back()) : subject;

    const mlir::Value value = generateValue(regionSubject);
    _opBuilder.create<mlir::db::CaseYield>(_opBuilder.getUnknownLoc(), value);

    _part = outerPart;
}

template <typename SelectionOp>
mlir::Value DBProgramGenerator::selectionResult(SelectionOp selection) {
    dropUnreadCarriedColumns(selection, selection.getColumnsToFilter());

    llvm::SmallVector<mlir::Value> selected;
    selection.collectSelectedValues(selected);

    mlir::Value result = selection.getResult();
    result.setType(allocColumnType(selectedColumnElement(_mlirCtxt, selected)));

    return result;
}

mlir::Value DBProgramGenerator::disjointComparison(mlir::Value lhs, mlir::Value rhs, bool equality) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    // A literal carries a value on every row it stands for, so only a column operand can be
    // absent, and a null test on the literal would meet its one row against the relation's
    llvm::SmallVector<mlir::Value, 2> columns;
    for (const mlir::Value operand : {lhs, rhs}) {
        if (!yieldsConstantColumn(operand, _constantColumns)) {
            columns.push_back(operand);
        }
    }

    if (columns.empty()) {
        return constantBool(!equality);
    }

    mlir::Value condition;
    for (const mlir::Value column : columns) {
        mlir::Value test;
        if (equality) {
            test = _opBuilder.create<mlir::db::IsNullOp>(loc, boolType, column).getResult();
        } else {
            test = _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, column).getResult();
        }

        if (!condition) {
            condition = test;
        } else if (equality) {
            condition = _opBuilder.create<mlir::db::OrOp>(loc, boolType, condition, test).getResult();
        } else {
            condition = _opBuilder.create<mlir::db::AndOp>(loc, boolType, condition, test).getResult();
        }
    }

    if (equality) {
        return _opBuilder.create<mlir::db::AndOp>(loc, boolType, condition, nullConstantColumn()).getResult();
    }

    return _opBuilder.create<mlir::db::OrOp>(loc, boolType, condition, nullConstantColumn()).getResult();
}

mlir::Value DBProgramGenerator::translateCaseTest(const Expr* subjectExpr,
                                                  mlir::Value subject,
                                                  const CaseExpr::Test& test) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const EvaluatedType subjectType = subjectExpr ? subjectExpr->getType() : EvaluatedType::Invalid;

    const bool subjectIsNull = subjectExpr
                               && (subjectType == EvaluatedType::Null || isUntypedNullColumn(subject));

    if (test._kind == CaseExpr::TestKind::IsNull) {
        if (subjectIsNull) {
            return constantBool(true);
        }

        return _opBuilder.create<mlir::db::IsNullOp>(loc, boolType, subject).getResult();
    } else if (test._kind == CaseExpr::TestKind::IsNotNull) {
        if (subjectIsNull) {
            return constantBool(false);
        }

        return _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, subject).getResult();
    }

    translateExpr(test._value);
    bioassert(_part._exprMap.contains(test._value), "CASE branch with no condition column.");

    const mlir::Value value = _part._exprMap.at(test._value);
    const EvaluatedType valueType = test._value->getType();

    // A test the analyzer typed Null is null on every row - a comparison with a null, an
    // ordering across types with no order between them - so its branch is never taken.
    // IS NULL above is how a Cypher CASE tests for a null
    const bool neverTaken = test._resultType == EvaluatedType::Null;
    const bool comparesDisjointTypes = subjectExpr && comparesAsDisjointTypes(subjectType, valueType);

    const bool testsEquality = test._operator == BinaryOperator::Equal
                               || test._operator == BinaryOperator::NotEqual;

    if (!subject) {
        return value;
    } else if (neverTaken) {
        return constantBool(false);
    } else if (comparesDisjointTypes && testsEquality) {
        return disjointComparison(subject, value, test._operator == BinaryOperator::Equal);
    }

    switch (test._operator) {
        case BinaryOperator::Equal:
            return _opBuilder.create<mlir::db::EqOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::NotEqual:
            return _opBuilder.create<mlir::db::NeqOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::LessThan:
            return _opBuilder.create<mlir::db::LtOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::GreaterThan:
            return _opBuilder.create<mlir::db::GtOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::LessThanOrEqual:
            return _opBuilder.create<mlir::db::LteOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::GreaterThanOrEqual:
            return _opBuilder.create<mlir::db::GteOp>(loc, boolType, subject, value).getResult();
        break;
        default:
            throwError(fmt::format("Unsupported comparison in a CASE branch: {}",
                                   BinaryOperatorDescription::value(test._operator)),
                       test._value);
        break;
    }
}

void DBProgramGenerator::translateLogicalExpr(const Expr* expr, const LogicalExpr* logicalExpr) {
    const LogicalOperator op = logicalExpr->getOperator();

    mlir::Value result;
    for (const Expr* operandExpr : logicalExpr->getOperands()) {
        translateExpr(operandExpr);
        bioassert(_part._exprMap.contains(operandExpr), "Logical operation with unknown operand.");

        const mlir::Value operand = _part._exprMap.at(operandExpr);
        result = result ? translateLogicalOperator(op, result, operand) : operand;
    }

    _part._exprMap[expr] = result;
}

mlir::Value DBProgramGenerator::translateLogicalOperator(LogicalOperator op, mlir::Value lhs, mlir::Value rhs) {
    // AND, OR and XOR read a null side as an unknown boolean, which the runtime truth
    // tables answer against the other side's value per row. With both sides null there is
    // no value to answer against and the result is null, whichever of the three it is
    if (isUntypedNullColumn(lhs) && isUntypedNullColumn(rhs)) {
        return nullConstantColumn();
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    switch (op) {
        case LogicalOperator::And:
            return _opBuilder.create<mlir::db::AndOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case LogicalOperator::Or:
            return _opBuilder.create<mlir::db::OrOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case LogicalOperator::Xor:
            return _opBuilder.create<mlir::db::XorOp>(loc, boolType, lhs, rhs).getResult();
        break;
        default:
            throwError("Unknown logical operator.");
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

    const BinaryOperator op = binExpr->getOperator();

    // Two types that can never hold equal values answer the equality without reading a row
    const bool comparesDisjointTypes = comparesAsDisjointTypes(lhsExpr->getType(), rhsExpr->getType());

    const bool testsKnownNull = lhsExpr->getType() == EvaluatedType::Null || isUntypedNullColumn(lhs);

    // The analyzer types Null an expression that is null on every row - a comparison with a
    // null, an ordering across types, arithmetic over a null - and the null names no column
    // the result could be carried in
    if (binExpr->getType() == EvaluatedType::Null) {
        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    switch (op) {
        case BinaryOperator::Equal:
            if (comparesDisjointTypes) {
                _part._exprMap[expr] = disjointComparison(lhs, rhs, /*equality=*/true);
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::EqOp>(loc, boolType, lhs, rhs).getResult();
            }
        break;
        case BinaryOperator::IsNull:
            if (testsKnownNull) {
                _part._exprMap[expr] = constantBool(true);
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::IsNullOp>(loc, boolType, lhs).getResult();
            }
        break;
        case BinaryOperator::IsNotNull:
            if (testsKnownNull) {
                _part._exprMap[expr] = constantBool(false);
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, lhs).getResult();
            }
        break;
        case BinaryOperator::Add: {
            const EvaluatedType resultType = binExpr->getType();

            // A type-erased cell joins a list as the list it holds or as one element of it,
            // which only its row says, so an add over one is decided row by row
            const bool addsACell = lhsExpr->getType() == EvaluatedType::Any
                                || rhsExpr->getType() == EvaluatedType::Any;
            const bool joinsAsText = resultType == EvaluatedType::String;
            const bool joinsAsList = resultType == EvaluatedType::List;
            const bool isConcatenation = !addsACell && (joinsAsText || joinsAsList);

            if (isConcatenation) {
                // A scalar concatenated with a list joins it as the list it stands for, so
                // both sides of the concatenation are lists
                const bool concatenatesLists = resultType == EvaluatedType::List;
                const bool lhsJoinsAList = concatenatesLists && lhsExpr->getType() != EvaluatedType::List;
                const bool rhsJoinsAList = concatenatesLists && rhsExpr->getType() != EvaluatedType::List;

                const mlir::Value lhsColumn = lhsJoinsAList ? joinedListOperand(lhs) : lhs;
                const mlir::Value rhsColumn = rhsJoinsAList ? joinedListOperand(rhs) : rhs;

                _part._exprMap[expr] = _opBuilder.create<mlir::db::ConcatOp>(loc, lhsColumn, rhsColumn).getResult();
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::AddOp>(loc, lhs, rhs).getResult();
            }
        }
        break;
        case BinaryOperator::Concat:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::ConcatOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::Sub:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::SubOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::Mult:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::MulOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::Div:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::DivOp>(loc, lhs, rhs).getResult();
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
            if (comparesDisjointTypes) {
                _part._exprMap[expr] = disjointComparison(lhs, rhs, /*equality=*/false);
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::NeqOp>(loc, boolType, lhs, rhs).getResult();
            }
        break;
        case BinaryOperator::Mod:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::ModOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::Pow:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::PowOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::In:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::InOp>(loc, boolType, lhs, rhs).getResult();
        break;

        case BinaryOperator::_SIZE:
        break;
    }
}

void DBProgramGenerator::translateListSliceExpr(const Expr* expr, const ListSliceExpr* slice) {
    // There is no list to read where the query sliced a null, and the null names no column
    // the result could be carried in: the analyzer types such a slice Null
    if (expr->getType() == EvaluatedType::Null) {
        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    const mlir::Value list = getOrTranslateExprColumn(slice->getBase());

    // A bound the query left out is no operand: the slice then runs from the start, or to
    // the end
    const Expr* const fromExpr = slice->getFrom();
    const Expr* const toExpr = slice->getTo();

    const mlir::Value from = fromExpr ? getOrTranslateExprColumn(fromExpr) : mlir::Value {};
    const mlir::Value to = toExpr ? getOrTranslateExprColumn(toExpr) : mlir::Value {};

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    _part._exprMap[expr] = _opBuilder.create<mlir::db::ListSlice>(loc, noneType, list, from, to).getResult();
}

void DBProgramGenerator::translateIndexExpr(const Expr* expr, const IndexExpr* indexExpr) {
    const Expr* base = indexExpr->getBase();
    const Expr* index = indexExpr->getIndexExpr();

    if (const Expr* dottedForm = indexExpr->getDottedForm()) {
        translateExpr(dottedForm);

        _part._exprMap[expr] = _part._exprMap.at(dottedForm);
        return;
    }

    // A null index reads nothing, whatever it names, so the analyzer types such a read Null.
    // Both operands are still emitted: an aggregate under one of them is not this layer's
    // to drop, and a pass prunes the columns nothing reads.
    if (expr->getType() == EvaluatedType::Null) {
        translateExpr(base);
        translateExpr(index);

        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    //translate a csv column read
    if (base->getType() == EvaluatedType::StringTable) {
        const mlir::Value fieldColumn = findVariableColumn(indexExpr->getCSVFieldDecl());

        if (!fieldColumn) {
            throwError("Only a constant index selects a CSV field: "
                       "row[i] with a computed index is not supported yet.",
                       expr);
        }

        _part._exprMap[expr] = fieldColumn;
        return;
    }

    translateExpr(base);
    translateExpr(index);

    bioassert(_part._exprMap.contains(base), "Index read with unknown base.");
    bioassert(_part._exprMap.contains(index), "Index read with unknown index.");

    const mlir::Value baseColumn = _part._exprMap.at(base);
    const mlir::Value indexColumn = _part._exprMap.at(index);
    const IndexExpr::ReadKind readKind = indexExpr->getReadKind();

    if (readKind == IndexExpr::ReadKind::MapKey) {
        _part._exprMap[expr] = emitDynamicMapKey(baseColumn, indexColumn);
        return;
    }

    //translate list or path indexing
    const mlir::Value container = readWalkEntities(base, baseColumn);

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    if (readKind == IndexExpr::ReadKind::ContainerElement) {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::ContainerIndex>(loc, noneType, container, indexColumn).getResult();
    } else {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::ListIndex>(loc, noneType, container, indexColumn).getResult();
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
            throwError("Unknown string operator.", expr);
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

mlir::DenseF32ArrayAttr DBProgramGenerator::embeddingLiteralAttr(const EmbeddingLiteral* literal) {
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
        case Literal::Kind::MAP:
            return translateMapLiteral(static_cast<const MapLiteral*>(literal));
        break;
        default:
            throw FatalException("Unsupported literal kind in WHERE clause expression.");
        break;
    }

    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());
    return _opBuilder.create<mlir::db::ConstantOp>(uloc, resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::translateParameterExpr(const ParameterExpr* parameterExpr) {
    const auto argumentIt = _parameterArguments.find(parameterExpr->getDecl());
    bioassert(argumentIt != _parameterArguments.end(), "A parameter reached codegen with no argument of main.");

    return argumentIt->second;
}

// A parameter with a value has the type its literal would have; a null has the null literal's
mlir::Type DBProgramGenerator::parameterType(const ParameterDecl* decl) {
    const std::optional<ValueType> valueType = decl->getValueType();
    if (!valueType) {
        return mlir::storage::NullableType::get(_mlirCtxt, mlir::NoneType::get(_mlirCtxt));
    } else if (*valueType == ValueType::List) {
        return listShapeType(_opBuilder, decl->getListShape());
    }

    return valueTypeToElementType(_opBuilder, *valueType);
}

// The column of a null in every row, the shape translateLiteralExpr gives the null literal
mlir::Value DBProgramGenerator::nullConstantColumn() {
    const mlir::Type nullableType = mlir::storage::NullableType::get(_mlirCtxt,
                                                                    mlir::NoneType::get(_mlirCtxt));
    const mlir::TypedAttr valueAttr = mlir::StringAttr::get("", nullableType);
    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::createCase(mlir::ValueRange conditions, mlir::ValueRange values, mlir::Value defaultValue) {
    llvm::SmallVector<mlir::Value> selected(values.begin(), values.end());
    if (defaultValue) {
        selected.push_back(defaultValue);
    }

    const mlir::db::ColumnType resultType = allocColumnType(selectedColumnElement(_mlirCtxt, selected));

    return _opBuilder.create<mlir::db::Case>(_opBuilder.getUnknownLoc(),
                                             resultType,
                                             conditions,
                                             values,
                                             defaultValue).getResult();
}

mlir::Value DBProgramGenerator::constantBool(bool value) {
    const mlir::TypedAttr valueAttr = _opBuilder.getBoolAttr(value);
    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::constantDouble(double value) {
    const mlir::TypedAttr valueAttr = _opBuilder.getF64FloatAttr(value);
    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::constantString(llvm::StringRef value) {
    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);
    const mlir::TypedAttr valueAttr = mlir::StringAttr::get(value, stringType);
    const mlir::db::ColumnType resultType = allocColumnType(stringType);

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::constantLabelList(llvm::ArrayRef<std::string> labels) {
    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);

    llvm::SmallVector<mlir::Attribute> elements;
    elements.reserve(labels.size());

    for (const std::string& label : labels) {
        elements.push_back(mlir::StringAttr::get(label, stringType));
    }

    mlir::db::ConstantOp constant = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                           _opBuilder.getArrayAttr(elements));

    return constant.getResult();
}

mlir::Type DBProgramGenerator::propertyValueType(ValueType valueType) {
    return mlir::storage::NullableType::get(_mlirCtxt, valueTypeToElementType(_opBuilder, valueType));
}

mlir::Value DBProgramGenerator::translatePropertyExpr(const PropertyExpr* propExpr) {
    const mlir::Value value = translatePropertyRead(propExpr);
    const std::optional<ValueComponent>& component = propExpr->getComponent();

    if (propExpr->readsAMapKey()) {
        return emitStaticMapKey(value, propExpr->getMapKey());
    } else if (component) {
        return emitComponent(*component, value);
    }

    return value;
}

mlir::Value DBProgramGenerator::translatePropertyLookupExpr(const PropertyLookupExpr* lookupExpr) {
    const EvaluatedType readType = lookupExpr->getType();
    if (readType == EvaluatedType::Null) {
        return nullConstantColumn();
    }

    const Expr* base = lookupExpr->getBase();
    translateExpr(base);

    bioassert(_part._exprMap.contains(base), "Property lookup with unknown base.");

    const mlir::Value baseColumn = _part._exprMap.at(base);
    const std::optional<ValueComponent>& component = lookupExpr->getComponent();

    if (lookupExpr->readsAMapKey()) {
        return emitStaticMapKey(baseColumn, lookupExpr->getPropName());
    }

    if (component) {
        return emitComponent(*component, baseColumn);
    } else if (readType == EvaluatedType::Any) {
        return emitStaticMapKey(baseColumn, lookupExpr->getPropName());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType resultType = allocColumnType(propertyValueType(lookupExpr->getValueType()));
    const mlir::StringAttr propAttr = _opBuilder.getStringAttr(lookupExpr->getPropName());

    if (base->getType() == EvaluatedType::NodePattern) {
        auto op = _opBuilder.create<mlir::db::GetNodeProperties>(loc,
                                                                 resultType,
                                                                 baseColumn,
                                                                 propAttr,
                                                                 mlir::Value(),
                                                                 false);
        return op.getResult();
    } else {
        auto op = _opBuilder.create<mlir::db::GetEdgeProperties>(loc,
                                                                 resultType,
                                                                 baseColumn,
                                                                 propAttr,
                                                                 mlir::Value(),
                                                                 false);
        return op.getResult();
    }
}

mlir::Value DBProgramGenerator::emitStaticMapKey(mlir::Value map, std::string_view key) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType cellType = allocColumnType(mlir::storage::AnyType::get(_mlirCtxt));
    const mlir::StringAttr keyAttr = _opBuilder.getStringAttr(llvm::StringRef(key.data(), key.size()));

    return _opBuilder.create<mlir::db::StaticMapKey>(loc, cellType, map, keyAttr).getResult();
}

mlir::Value DBProgramGenerator::emitDynamicMapKey(mlir::Value map, mlir::Value key) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType cellType = allocColumnType(mlir::storage::AnyType::get(_mlirCtxt));

    return _opBuilder.create<mlir::db::DynamicMapKey>(loc, cellType, map, key).getResult();
}

mlir::Value DBProgramGenerator::emitComponent(const ValueComponent& component, mlir::Value value) {
    return std::visit([this, value](auto part) { return emitComponent(part, value); }, component);
}

mlir::Value DBProgramGenerator::emitComponent(DateTimePart part, mlir::Value instant) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (part) {
        case DateTimePart::Year:
            return _opBuilder.create<mlir::db::DateTimeYear>(loc, instant).getResult();
        break;

        case DateTimePart::Month:
            return _opBuilder.create<mlir::db::DateTimeMonth>(loc, instant).getResult();
        break;

        case DateTimePart::Day:
            return _opBuilder.create<mlir::db::DateTimeDay>(loc, instant).getResult();
        break;

        case DateTimePart::Hour:
            return _opBuilder.create<mlir::db::DateTimeHour>(loc, instant).getResult();
        break;

        case DateTimePart::Minute:
            return _opBuilder.create<mlir::db::DateTimeMinute>(loc, instant).getResult();
        break;

        case DateTimePart::Second:
            return _opBuilder.create<mlir::db::DateTimeSecond>(loc, instant).getResult();
        break;

        case DateTimePart::Millisecond:
            return _opBuilder.create<mlir::db::DateTimeMillisecond>(loc, instant).getResult();
        break;

        case DateTimePart::Microsecond:
            return _opBuilder.create<mlir::db::DateTimeMicrosecond>(loc, instant).getResult();
        break;
    }

    return mlir::Value();
}

mlir::Value DBProgramGenerator::emitComponent(PointPart part, mlir::Value point) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (part) {
        case PointPart::X:
            return _opBuilder.create<mlir::db::PointX>(loc, point).getResult();
        break;

        case PointPart::Y:
            return _opBuilder.create<mlir::db::PointY>(loc, point).getResult();
        break;

        case PointPart::Z:
            return _opBuilder.create<mlir::db::PointZ>(loc, point).getResult();
        break;

        case PointPart::Longitude:
            return _opBuilder.create<mlir::db::PointLongitude>(loc, point).getResult();
        break;

        case PointPart::Latitude:
            return _opBuilder.create<mlir::db::PointLatitude>(loc, point).getResult();
        break;

        case PointPart::Height:
            return _opBuilder.create<mlir::db::PointHeight>(loc, point).getResult();
        break;

        case PointPart::CRS:
            return _opBuilder.create<mlir::db::PointCRSName>(loc, point).getResult();
        break;

        case PointPart::SRID:
            return _opBuilder.create<mlir::db::PointSRID>(loc, point).getResult();
        break;
    }

    return mlir::Value();
}

mlir::Value DBProgramGenerator::emitComponent(DurationPart part, mlir::Value duration) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (part) {
        case DurationPart::Years:
            return _opBuilder.create<mlir::db::DurationYears>(loc, duration).getResult();
        break;

        case DurationPart::Quarters:
            return _opBuilder.create<mlir::db::DurationQuarters>(loc, duration).getResult();
        break;

        case DurationPart::Months:
            return _opBuilder.create<mlir::db::DurationMonths>(loc, duration).getResult();
        break;

        case DurationPart::Weeks:
            return _opBuilder.create<mlir::db::DurationWeeks>(loc, duration).getResult();
        break;

        case DurationPart::Days:
            return _opBuilder.create<mlir::db::DurationDays>(loc, duration).getResult();
        break;

        case DurationPart::Hours:
            return _opBuilder.create<mlir::db::DurationHours>(loc, duration).getResult();
        break;

        case DurationPart::Minutes:
            return _opBuilder.create<mlir::db::DurationMinutes>(loc, duration).getResult();
        break;

        case DurationPart::Seconds:
            return _opBuilder.create<mlir::db::DurationSeconds>(loc, duration).getResult();
        break;

        case DurationPart::Milliseconds:
            return _opBuilder.create<mlir::db::DurationMilliseconds>(loc, duration).getResult();
        break;

        case DurationPart::Microseconds:
            return _opBuilder.create<mlir::db::DurationMicroseconds>(loc, duration).getResult();
        break;

        case DurationPart::QuartersOfYear:
            return _opBuilder.create<mlir::db::DurationQuartersOfYear>(loc, duration).getResult();
        break;

        case DurationPart::MonthsOfYear:
            return _opBuilder.create<mlir::db::DurationMonthsOfYear>(loc, duration).getResult();
        break;

        case DurationPart::MonthsOfQuarter:
            return _opBuilder.create<mlir::db::DurationMonthsOfQuarter>(loc, duration).getResult();
        break;

        case DurationPart::DaysOfWeek:
            return _opBuilder.create<mlir::db::DurationDaysOfWeek>(loc, duration).getResult();
        break;

        case DurationPart::MinutesOfHour:
            return _opBuilder.create<mlir::db::DurationMinutesOfHour>(loc, duration).getResult();
        break;

        case DurationPart::SecondsOfMinute:
            return _opBuilder.create<mlir::db::DurationSecondsOfMinute>(loc, duration).getResult();
        break;

        case DurationPart::MillisecondsOfSecond:
            return _opBuilder.create<mlir::db::DurationMillisecondsOfSecond>(loc, duration).getResult();
        break;

        case DurationPart::MicrosecondsOfSecond:
            return _opBuilder.create<mlir::db::DurationMicrosecondsOfSecond>(loc, duration).getResult();
        break;
    }

    return mlir::Value();
}

mlir::Value DBProgramGenerator::translatePropertyRead(const PropertyExpr* propExpr) {
    const VarDecl* entityDecl = propExpr->getEntityVarDecl();
    const std::string_view varName = entityDecl->getName();
    const std::string_view propName = propExpr->getPropName();

    // d.year, d.hours, p.x and m.key name no property: the value the component or the key
    // is read off is the column the variable itself was bound to
    const EvaluatedType entityType = entityDecl->getType();

    const bool readsOffTheVariableItself = entityType == EvaluatedType::DateTime
                                        || entityType == EvaluatedType::Duration
                                        || entityType == EvaluatedType::Point
                                        || entityType == EvaluatedType::Map
                                        || entityType == EvaluatedType::Any;

    if (readsOffTheVariableItself) {
        const auto projectedIt = _part._projectedColumns.find(entityDecl);
        const mlir::Value boundColumn = projectedIt != _part._projectedColumns.end()
                                      ? projectedIt->second
                                      : resolveEntityColumn(entityDecl);

        bioassert(boundColumn, "Dotted read on unknown variable: {}", varName);

        return boundColumn;
    }

    // A header access reads a field of a loaded record rather than a property of an
    // entity: the load published its column under the declaration the access carries
    if (propExpr->isStringTableHeaderAccess()) {
        const mlir::Value fieldColumn = findVariableColumn(propExpr->getCSVFieldDecl());
        bioassert(fieldColumn, "CSV header access on a row no load published: {}.{}", varName, propName);

        return fieldColumn;
    }

    // No property in the graph carries the name - the analyzer typed the read null for
    // it - so there is nothing to fetch and every row reads null. A property only a write
    // of tagged cells creates is typed as the program runs, so its value is read by name,
    // as a tagged cell.
    const EvaluatedType readType = propExpr->getType();
    if (readType == EvaluatedType::Null) {
        return nullConstantColumn();
    } else if (readType == EvaluatedType::Any) {
        const mlir::Value entityColumn = resolveEntityColumn(entityDecl);
        bioassert(entityColumn, "Property access on unknown variable: {}", varName);

        return emitStaticMapKey(entityColumn, propName);
    }

    // The value of a property a CREATE wrote is the column it wrote it from, which saves
    // the read the detour through the write buffer the fetch below takes. A MERGE's rows
    // are a mixture, so they go through that fetch, which reads each row where its own
    // entity lives.
    const auto createdIt = _part._createdEntities.find(entityDecl);
    const bool writtenByACreate = createdIt != _part._createdEntities.end() && !createdIt->second._pending;
    if (writtenByACreate) {
        const auto& properties = createdIt->second._properties;
        const auto propertyIt = properties.find(propName);

        if (propertyIt != end(properties)) {
            return propertyIt->second;
        }
    }

    const mlir::Value entityColumn = resolveEntityColumn(entityDecl);

    bioassert(entityColumn, "WHERE clause property access on unknown variable: {}", varName);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType resultType = allocColumnType(propertyValueType(propExpr->getValueType()));
    const mlir::StringAttr propAttr = _opBuilder.getStringAttr(propName);

    const bool isNode = entityType == EvaluatedType::NodePattern;
    const bool isEdge = entityType == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Property access on non-entity variable: {}", varName);

    const mlir::Value pending = findPendingMask(entityDecl);
    const bool allPending = isPendingThroughout(entityDecl);

    if (isNode) {
        auto op = _opBuilder.create<mlir::db::GetNodeProperties>(loc,
                                                                 resultType,
                                                                 entityColumn,
                                                                 propAttr,
                                                                 pending,
                                                                 allPending);
        return op.getResult();
    } else {
        auto op = _opBuilder.create<mlir::db::GetEdgeProperties>(loc,
                                                                 resultType,
                                                                 entityColumn,
                                                                 propAttr,
                                                                 pending,
                                                                 allPending);
        return op.getResult();
    }
}

// A node carries every label its test names, an edge the single type it was written with
bool DBProgramGenerator::writtenEntityPasses(const PartScope::WrittenEntity& written,
                                             const LabelExpr* labelExpr,
                                             bool isNode) {
    const LabelExpr::Operands& operands = labelExpr->getOperands();
    const auto passes = [&written, isNode](const LabelExpr* operand) {
        return writtenEntityPasses(written, operand, isNode);
    };

    switch (labelExpr->getKind()) {
        case LabelExpr::Kind::LABELS:
            return std::ranges::all_of(*labelExpr->getLabels(), [&written, isNode](const Symbol* label) {
                const std::string_view name = label->getName();
                return isNode ? std::ranges::find(written._labels, name) != end(written._labels)
                              : name == written._edgeType;
            });
        break;
        case LabelExpr::Kind::NOT:
            return !passes(operands.front());
        break;
        case LabelExpr::Kind::AND:
            return std::ranges::all_of(operands, passes);
        break;
        case LabelExpr::Kind::OR:
            return std::ranges::any_of(operands, passes);
        break;
    }

    return false;
}

mlir::Value DBProgramGenerator::translateEntityTypeExpr(const EntityTypeExpr* typeExpr) {
    if (typeExpr->getType() == EvaluatedType::Null) {
        return nullConstantColumn();
    }

    const LabelExpr* labelExpr = typeExpr->getLabelExpr();

    if (const Expr* operand = typeExpr->getOperand()) {
        translateExpr(operand);
        bioassert(_part._exprMap.contains(operand), "Type test with unknown operand.");

        const mlir::Value entityColumn = _part._exprMap.at(operand);

        if (operand->getType() == EvaluatedType::EdgePattern) {
            return checkEdgeTypeExpr(fetchEdgeTypeColumn(entityColumn), labelExpr, true);
        }

        return checkNodeLabelExpr(entityColumn, labelExpr, true);
    }

    const VarDecl* entityDecl = typeExpr->getEntityVarDecl();
    const std::string_view varName = entityDecl->getName();

    const EvaluatedType entityType = entityDecl->getType();
    const bool isNode = entityType == EvaluatedType::NodePattern;
    const bool isEdge = entityType == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Type test on non-entity variable: {}", varName);

    // The labels and the type the CREATE spelled answer a test on what the query wrote
    // without a read, laid out over the rows the entity carries. A parameter on either
    // side has no answer before it is bound, so that test reads the pending entity instead
    const PartScope::WrittenEntity* written = findWrittenEntity(entityDecl);
    const bool writtenTypesKnown = written && (isNode ? !written->_labels.empty() : !written->_edgeType.empty());
    const bool writtenTypesUnknown = written && !(isNode ? written->_labelsKnown : written->_edgeTypeKnown);
    const bool boundAtRuntime = labelExpr->holdsParameter() || writtenTypesUnknown;
    if (writtenTypesKnown && !boundAtRuntime) {
        const mlir::Value answer = constantBool(writtenEntityPasses(*written, labelExpr, isNode));

        return _opBuilder.create<mlir::db::BroadcastConstant>(_opBuilder.getUnknownLoc(),
                                                              answer,
                                                              resolveEntityColumn(entityDecl)).getResult();
    }

    if (isEdge) {
        return checkEdgeTypeExpr(resolveOrFetchEdgeTypeColumn(entityDecl, varName), labelExpr, true);
    }

    const mlir::Value nodeColumn = resolveEntityColumn(entityDecl);
    bioassert(nodeColumn, "Label test on unknown variable: {}", varName);

    return checkNodeLabelExpr(nodeColumn, labelExpr, true);
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

void DBProgramGenerator::translateCoalesce(const Expr* expr, const ExprChain* args) {
    bioassert(args && !args->empty(), "coalesce() with no arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const llvm::SmallVector<mlir::Value> carried(carrySet._columns.begin(), carrySet._columns.end());

    auto coalesce = _opBuilder.create<mlir::db::Coalesce>(_opBuilder.getUnknownLoc(),
                                                          noneType,
                                                          carried,
                                                          static_cast<unsigned>(argExprs.size()));
    const mlir::MutableArrayRef<mlir::Region> regions = coalesce.getArguments();

    for (size_t argumentIndex = 0; argumentIndex < argExprs.size(); argumentIndex++) {
        const Expr* const argumentExpr = argExprs[argumentIndex];

        generateCaseRegion(regions[argumentIndex], carried, carrySet, mlir::Value {}, [&](mlir::Value) {
            return translateArg(argumentExpr);
        });
    }

    _part._exprMap[expr] = selectionResult(coalesce);
}
