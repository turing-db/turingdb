#include "CypherASTComparator.h"

#include <algorithm>
#include <bit>
#include <functional>
#include <iterator>
#include <variant>
#include <vector>

#include <spdlog/fmt/bundled/format.h>

#include "ChangeQuery.h"
#include "CreateEdgePropertyIndexQuery.h"
#include "CreateGraphQuery.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateVectorIndexQuery.h"
#include "CypherAST.h"
#include "DeleteVectorIndexQuery.h"
#include "DropIndexQuery.h"
#include "EdgePattern.h"
#include "EmbeddingsSpec.h"
#include "TypedPropertySpec.h"
#include "ExplainRequest.h"
#include "FunctionInvocation.h"
#include "InstallExtensionQuery.h"
#include "LabelExpr.h"
#include "Literal.h"
#include "LoadCommitQuery.h"
#include "LoadEmbeddingQuery.h"
#include "LoadGMLQuery.h"
#include "LoadGraphQuery.h"
#include "LoadJsonlQuery.h"
#include "LoadParquetQuery.h"
#include "LoadVectorQuery.h"
#include "NodePattern.h"
#include "Parameter.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "QualifiedName.h"
#include "QuantifiedPath.h"
#include "QueryCommand.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "SinglePartQuery.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "WhereClause.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "expr/All.h"
#include "stmt/CallStmt.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/DeleteStmt.h"
#include "stmt/Limit.h"
#include "stmt/LoadCSVStmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/MergeStmt.h"
#include "stmt/NextSegmentStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/RemoveStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/SetItem.h"
#include "stmt/SetStmt.h"
#include "stmt/ShortestPathStmt.h"
#include "stmt/Skip.h"
#include "stmt/StmtContainer.h"
#include "stmt/UnwindStmt.h"
#include "stmt/VectorSearchStmt.h"
#include "stmt/WithStmt.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view NULL_TEXT = "null";

constexpr ExplainStage EXPLAIN_STAGES[] = {
    ExplainStage::AST,
    ExplainStage::VDG,
    ExplainStage::CODEGEN,
    ExplainStage::PASSES,
    ExplainStage::DB,
    ExplainStage::NL,
};

class PathScope {
public:
    PathScope(std::string& path, std::string_view field)
        : _path(path),
        _length(path.size())
    {
        if (!_path.empty()) {
            _path += " > ";
        }

        _path += field;
    }

    ~PathScope() {
        _path.resize(_length);
    }

    PathScope(const PathScope&) = delete;
    PathScope& operator=(const PathScope&) = delete;

private:
    std::string& _path;
    size_t _length {0};
};

void formatIndexedField(std::string_view field, size_t index, std::string& label) {
    label.clear();
    fmt::format_to(std::back_inserter(label), "{}[{}]", field, index);
}

std::string_view getClassName(const QueryCommand* query) {
    if (!query) {
        return NULL_TEXT;
    }

    switch (query->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            return "SinglePartQuery";
        break;
        case QueryCommand::Kind::LOAD_GRAPH_QUERY:
            return "LoadGraphQuery";
        break;
        case QueryCommand::Kind::LOAD_JSONL_QUERY:
            return "LoadJsonlQuery";
        break;
        case QueryCommand::Kind::CHANGE_QUERY:
            return "ChangeQuery";
        break;
        case QueryCommand::Kind::COMMIT_QUERY:
            return "CommitQuery";
        break;
        case QueryCommand::Kind::LIST_GRAPH_QUERY:
            return "ListGraphQuery";
        break;
        case QueryCommand::Kind::LIST_AVAILABLE_GRAPHS_QUERY:
            return "ListAvailableGraphsQuery";
        break;
        case QueryCommand::Kind::CREATE_GRAPH_QUERY:
            return "CreateGraphQuery";
        break;
        case QueryCommand::Kind::LOAD_GML_QUERY:
            return "LoadGMLQuery";
        break;
        case QueryCommand::Kind::LOAD_PARQUET_QUERY:
            return "LoadParquetQuery";
        break;
        case QueryCommand::Kind::S3_CONNECT_QUERY:
            return "S3ConnectQuery";
        break;
        case QueryCommand::Kind::S3_TRANSFER_QUERY:
            return "S3TransferQuery";
        break;
        case QueryCommand::Kind::SHOW_PROCEDURES_QUERY:
            return "ShowProceduresQuery";
        break;
        case QueryCommand::Kind::CREATE_VECTOR_INDEX_QUERY:
            return "CreateVectorIndexQuery";
        break;
        case QueryCommand::Kind::LOAD_VECTOR_QUERY:
            return "LoadVectorQuery";
        break;
        case QueryCommand::Kind::LOAD_EMBEDDING_QUERY:
            return "LoadEmbeddingQuery";
        break;
        case QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY:
            return "DeleteVectorIndexQuery";
        break;
        case QueryCommand::Kind::SHOW_VECTOR_INDEXES_QUERY:
            return "ShowVectorIndexesQuery";
        break;
        case QueryCommand::Kind::LOAD_COMMIT_QUERY:
            return "LoadCommitQuery";
        break;
        case QueryCommand::Kind::INSTALL_EXTENSION_QUERY:
            return "InstallExtensionQuery";
        break;
        case QueryCommand::Kind::SHOW_EXTENSIONS_QUERY:
            return "ShowExtensionsQuery";
        break;
        case QueryCommand::Kind::CREATE_NODE_PROPERTY_INDEX_QUERY:
            return "CreateNodePropertyIndexQuery";
        break;
        case QueryCommand::Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY:
            return "CreateEdgePropertyIndexQuery";
        break;
        case QueryCommand::Kind::DROP_INDEX_QUERY:
            return "DropIndexQuery";
        break;
        case QueryCommand::Kind::MERGE_DATAPARTS_QUERY:
            return "MergeDataPartsQuery";
        break;
        case QueryCommand::Kind::UNION_QUERY:
            return "UnionQuery";
        break;
        case QueryCommand::Kind::CONDITIONAL_QUERY:
            return "ConditionalQuery";
        break;
    }

    return "QueryCommand";
}

std::string_view getClassName(const Stmt* stmt) {
    if (!stmt) {
        return NULL_TEXT;
    }

    switch (stmt->getKind()) {
        case Stmt::Kind::MATCH:
            return "MatchStmt";
        break;
        case Stmt::Kind::CALL:
            return "CallStmt";
        break;
        case Stmt::Kind::CREATE:
            return "CreateStmt";
        break;
        case Stmt::Kind::MERGE:
            return "MergeStmt";
        break;
        case Stmt::Kind::SET:
            return "SetStmt";
        break;
        case Stmt::Kind::REMOVE:
            return "RemoveStmt";
        break;
        case Stmt::Kind::DELETE:
            return "DeleteStmt";
        break;
        case Stmt::Kind::RETURN:
            return "ReturnStmt";
        break;
        case Stmt::Kind::SHORTESTPATH:
            return "ShortestPathStmt";
        break;
        case Stmt::Kind::LOAD_CSV:
            return "LoadCSVStmt";
        break;
        case Stmt::Kind::VECTOR_SEARCH:
            return "VectorSearchStmt";
        break;
        case Stmt::Kind::UNWIND:
            return "UnwindStmt";
        break;
        case Stmt::Kind::WITH:
            return "WithStmt";
        break;
        case Stmt::Kind::CALL_SUBQUERY:
            return "CallSubqueryStmt";
        break;
        case Stmt::Kind::NEXT_SEGMENT:
            return "NextSegmentStmt";
        break;
    }

    return "Stmt";
}

std::string_view getClassName(const Expr* expr) {
    if (!expr) {
        return NULL_TEXT;
    }

    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            return "BinaryExpr";
        break;
        case Expr::Kind::LOGICAL:
            return "LogicalExpr";
        break;
        case Expr::Kind::UNARY:
            return "UnaryExpr";
        break;
        case Expr::Kind::STRING:
            return "StringExpr";
        break;
        case Expr::Kind::ENTITY_TYPES:
            return "EntityTypeExpr";
        break;
        case Expr::Kind::PROPERTY:
            return "PropertyExpr";
        break;
        case Expr::Kind::PROPERTY_LOOKUP:
            return "PropertyLookupExpr";
        break;
        case Expr::Kind::SYMBOL:
            return "SymbolExpr";
        break;
        case Expr::Kind::LITERAL:
            return "LiteralExpr";
        break;
        case Expr::Kind::FUNCTION_INVOCATION:
            return "FunctionInvocationExpr";
        break;
        case Expr::Kind::INDEX:
            return "IndexExpr";
        break;
        case Expr::Kind::LIST:
            return "ListExpr";
        break;
        case Expr::Kind::LIST_SLICE:
            return "ListSliceExpr";
        break;
        case Expr::Kind::LIST_COMPREHENSION:
            return "ListComprehensionExpr";
        break;
        case Expr::Kind::PATTERN_COMPREHENSION:
            return "PatternComprehensionExpr";
        break;
        case Expr::Kind::LIST_PREDICATE:
            return "ListPredicateExpr";
        break;
        case Expr::Kind::REDUCE:
            return "ReduceExpr";
        break;
        case Expr::Kind::ALL_REDUCE:
            return "AllReduceExpr";
        break;
        case Expr::Kind::MAP_PROJECTION:
            return "MapProjectionExpr";
        break;
        case Expr::Kind::CASE:
            return "CaseExpr";
        break;
        case Expr::Kind::EXISTS:
            return "ExistsExpr";
        break;
        case Expr::Kind::COUNT_SUBQUERY:
            return "CountSubqueryExpr";
        break;
        case Expr::Kind::COLLECT_SUBQUERY:
            return "CollectSubqueryExpr";
        break;
        case Expr::Kind::PARAMETER:
            return "ParameterExpr";
        break;
        case Expr::Kind::_SIZE:
        break;
    }

    return "Expr";
}

std::string_view getClassName(const Literal* literal) {
    if (!literal) {
        return NULL_TEXT;
    }

    switch (literal->getKind()) {
        case Literal::Kind::NULL_LITERAL:
            return "NullLiteral";
        break;
        case Literal::Kind::BOOL:
            return "BoolLiteral";
        break;
        case Literal::Kind::INTEGER:
            return "IntegerLiteral";
        break;
        case Literal::Kind::DOUBLE:
            return "DoubleLiteral";
        break;
        case Literal::Kind::STRING:
            return "StringLiteral";
        break;
        case Literal::Kind::CHAR:
            return "CharLiteral";
        break;
        case Literal::Kind::LIST:
            return "ListLiteral";
        break;
        case Literal::Kind::MAP:
            return "MapLiteral";
        break;
        case Literal::Kind::EMBEDDING:
            return "EmbeddingLiteral";
        break;
        case Literal::Kind::WILDCARD:
            return "WildcardLiteral";
        break;
        case Literal::Kind::NORMAL_FORM:
            return "NormalFormLiteral";
        break;
    }

    return "Literal";
}

std::string_view getClassName(const EntityPattern* entity) {
    if (!entity) {
        return NULL_TEXT;
    } else if (dynamic_cast<const NodePattern*>(entity)) {
        return "NodePattern";
    }

    return "EdgePattern";
}

std::string_view getClassName(const LabelExpr* labelExpr) {
    if (!labelExpr) {
        return NULL_TEXT;
    }

    switch (labelExpr->getKind()) {
        case LabelExpr::Kind::LABELS:
            return "LABELS";
        break;
        case LabelExpr::Kind::NOT:
            return "NOT";
        break;
        case LabelExpr::Kind::AND:
            return "AND";
        break;
        case LabelExpr::Kind::OR:
            return "OR";
        break;
    }

    return "LabelExpr";
}

std::string_view getClassName(const SetItem* item) {
    if (!item) {
        return NULL_TEXT;
    }

    const SetItem::Variant& variant = item->item();

    if (std::holds_alternative<SetItem::PropertyExprAssign>(variant)) {
        return "PropertyExprAssign";
    } else if (std::holds_alternative<SetItem::EntityPropertiesAssign>(variant)) {
        return "EntityPropertiesAssign";
    }

    return "SymbolEntityTypes";
}

std::string_view getClassName(const Projection::ReturnItem& item) {
    Expr* const* expr = std::get_if<Expr*>(&item);
    if (expr) {
        return getClassName(*expr);
    }

    return "VarDecl";
}

std::string_view getEnumName(BinaryOperator binaryOperator) {
    return BinaryOperatorDescription::value(binaryOperator);
}

std::string_view getEnumName(LogicalOperator logicalOperator) {
    return LogicalOperatorDescription::value(logicalOperator);
}

std::string_view getEnumName(UnaryOperator unaryOperator) {
    return UnaryOperatorDescription::value(unaryOperator);
}

std::string_view getEnumName(StringOperator stringOperator) {
    return StringOperatorDescription::value(stringOperator);
}

std::string_view getEnumName(UnicodeNormalForm form) {
    return UnicodeNormalFormName::value(form);
}

std::string_view getEnumName(vec::DistanceMetric metric) {
    return vec::DistanceMetricName::value(metric);
}

std::string_view getEnumName(vec::IndexType indexType) {
    return vec::IndexTypeName::value(indexType);
}

std::string_view getEnumName(OrderByType orderByType) {
    switch (orderByType) {
        case OrderByType::ASC:
            return "ASC";
        break;
        case OrderByType::DESC:
            return "DESC";
        break;
    }

    return "OrderByType";
}

std::string_view getEnumName(EdgePattern::Direction direction) {
    switch (direction) {
        case EdgePattern::Direction::Undirected:
            return "Undirected";
        break;
        case EdgePattern::Direction::Backward:
            return "Backward";
        break;
        case EdgePattern::Direction::Forward:
            return "Forward";
        break;
    }

    return "Direction";
}

std::string_view getEnumName(ChangeOp changeOp) {
    switch (changeOp) {
        case ChangeOp::NEW:
            return "NEW";
        break;
        case ChangeOp::SUBMIT:
            return "SUBMIT";
        break;
        case ChangeOp::DELETE:
            return "DELETE";
        break;
        case ChangeOp::LIST:
            return "LIST";
        break;
    }

    return "ChangeOp";
}

std::string_view getEnumName(S3TransferQuery::Direction direction) {
    switch (direction) {
        case S3TransferQuery::Direction::PULL:
            return "PULL";
        break;
        case S3TransferQuery::Direction::PUSH:
            return "PUSH";
        break;
    }

    return "Direction";
}

std::string_view getEnumName(ListPredicateExpr::Quantifier quantifier) {
    switch (quantifier) {
        case ListPredicateExpr::Quantifier::All:
            return "All";
        break;
        case ListPredicateExpr::Quantifier::Any:
            return "Any";
        break;
        case ListPredicateExpr::Quantifier::None:
            return "None";
        break;
        case ListPredicateExpr::Quantifier::Single:
            return "Single";
        break;
    }

    return "Quantifier";
}

std::string_view getEnumName(MapProjectionExpr::ItemKind itemKind) {
    switch (itemKind) {
        case MapProjectionExpr::ItemKind::Property:
            return "Property";
        break;
        case MapProjectionExpr::ItemKind::AllProperties:
            return "AllProperties";
        break;
        case MapProjectionExpr::ItemKind::Literal:
            return "Literal";
        break;
        case MapProjectionExpr::ItemKind::Variable:
            return "Variable";
        break;
    }

    return "ItemKind";
}

std::string_view getEnumName(CaseExpr::TestKind testKind) {
    switch (testKind) {
        case CaseExpr::TestKind::Value:
            return "Value";
        break;
        case CaseExpr::TestKind::Comparison:
            return "Comparison";
        break;
        case CaseExpr::TestKind::IsNull:
            return "IsNull";
        break;
        case CaseExpr::TestKind::IsNotNull:
            return "IsNotNull";
        break;
    }

    return "TestKind";
}

bool haveSameSymbol(const Symbol* expected, const Symbol* actual) {
    if (!expected || !actual) {
        return !expected && !actual;
    }

    return expected->getName() == actual->getName() && expected->getOriginalName() == actual->getOriginalName();
}

void renderQuoted(std::string_view text, std::string& rendered) {
    rendered += '"';
    rendered += text;
    rendered += '"';
}

void renderSymbol(const Symbol* symbol, std::string& rendered) {
    if (!symbol) {
        rendered += NULL_TEXT;
        return;
    }

    const std::string_view name = symbol->getName();
    const std::string_view originalName = symbol->getOriginalName();

    if (originalName != name) {
        renderQuoted(originalName, rendered);
        rendered += " AS ";
    }

    renderQuoted(name, rendered);
}

void renderParameter(const Parameter* parameter, std::string& rendered) {
    if (!parameter) {
        rendered += NULL_TEXT;
        return;
    }

    rendered += '$';
    rendered += parameter->getName();
}

void renderBound(int64_t bound, std::string& rendered) {
    if (bound == QuantifiedPath::UNBOUNDED) {
        rendered += "unbounded";
        return;
    }

    fmt::format_to(std::back_inserter(rendered), "{}", bound);
}

void renderTypedNames(const std::vector<std::pair<std::string_view, ValueType>>& names, std::string& rendered) {
    rendered += '{';

    for (size_t index = 0; index < names.size(); index++) {
        if (index > 0) {
            rendered += ", ";
        }

        renderQuoted(names[index].first, rendered);
        rendered += ": ";
        rendered += ValueTypeName::value(names[index].second);
    }

    rendered += '}';
}

void renderEmbeddings(const std::vector<std::pair<std::string_view, size_t>>& embeddings, std::string& rendered) {
    rendered += '{';

    for (size_t index = 0; index < embeddings.size(); index++) {
        if (index > 0) {
            rendered += ", ";
        }

        renderQuoted(embeddings[index].first, rendered);
        fmt::format_to(std::back_inserter(rendered), ": {}", embeddings[index].second);
    }

    rendered += '}';
}

}

CypherASTComparator::CypherASTComparator(std::string& difference)
    : _difference(difference)
{
}

CypherASTComparator::~CypherASTComparator() {
}

bool CypherASTComparator::compare(const CypherAST& expected, const CypherAST& actual, std::string& difference) {
    difference.clear();

    CypherASTComparator comparator(difference);

    return comparator.compareEach("queries", expected.queries(), actual.queries(), &CypherASTComparator::compareQuery)
           && comparator.compareExplainRequest("explainRequest", expected.getExplainRequest(), actual.getExplainRequest());
}

CypherASTComparator::Visit CypherASTComparator::visitNode(const void* expected,
                                                          const void* actual,
                                                          std::string_view expectedClass,
                                                          std::string_view actualClass) {
    if (expectedClass != actualClass) {
        reportDifference(expectedClass, actualClass);
        return Visit::DIFFERENT;
    } else if (!expected) {
        return Visit::EQUAL;
    }

    return pairNodes(expected, actual);
}

CypherASTComparator::Visit CypherASTComparator::visitNode(const void* expected, const void* actual, std::string_view className) {
    const std::string_view expectedClass = expected ? className : NULL_TEXT;
    const std::string_view actualClass = actual ? className : NULL_TEXT;

    return visitNode(expected, actual, expectedClass, actualClass);
}

CypherASTComparator::Visit CypherASTComparator::visitField(std::string_view field,
                                                           const void* expected,
                                                           const void* actual,
                                                           std::string_view expectedClass,
                                                           std::string_view actualClass) {
    const PathScope scope(_path, field);

    return visitNode(expected, actual, expectedClass, actualClass);
}

CypherASTComparator::Visit CypherASTComparator::visitField(std::string_view field,
                                                           const void* expected,
                                                           const void* actual,
                                                           std::string_view className) {
    const PathScope scope(_path, field);

    return visitNode(expected, actual, className);
}

CypherASTComparator::Visit CypherASTComparator::pairNodes(const void* expected, const void* actual) {
    const auto expectedPairing = _expectedPairings.find(expected);
    const auto actualPairing = _actualPairings.find(actual);

    const bool expectedSeen = expectedPairing != _expectedPairings.end();
    const bool actualSeen = actualPairing != _actualPairings.end();

    if (expectedSeen && expectedPairing->second._actual != actual) {
        reportDifference("the same node as at " + expectedPairing->second._path, "a different node");
        return Visit::DIFFERENT;
    } else if (actualSeen && actualPairing->second != expected) {
        const std::string& firstPath = _expectedPairings.at(actualPairing->second)._path;
        reportDifference("a different node from the one at " + firstPath, "the same node");
        return Visit::DIFFERENT;
    } else if (expectedSeen) {
        return Visit::EQUAL;
    }

    _expectedPairings.emplace(expected, Pairing {._actual = actual, ._path = _path});
    _actualPairings.emplace(actual, expected);

    return Visit::PENDING;
}

void CypherASTComparator::appendClassName(std::string_view className) {
    _path += ' ';
    _path += className;
}

void CypherASTComparator::reportDifference(std::string_view expected, std::string_view actual) {
    _difference = _path;
    _difference += ": expected ";
    _difference += expected;
    _difference += ", actual ";
    _difference += actual;
}

template <typename Elements, typename CompareElement>
bool CypherASTComparator::compareEach(std::string_view field, const Elements& expected, const Elements& actual, CompareElement compareElement) {
    if (!compareSize(field, expected.size(), actual.size())) {
        return false;
    }

    std::string label;
    auto actualElement = actual.begin();
    size_t index = 0;

    for (const auto& expectedElement : expected) {
        formatIndexedField(field, index, label);

        if (!std::invoke(compareElement, this, label, expectedElement, *actualElement)) {
            return false;
        }

        ++actualElement;
        index++;
    }

    return true;
}

template <typename Enum>
bool CypherASTComparator::compareEnum(std::string_view field, Enum expected, Enum actual) {
    if (expected == actual) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(getEnumName(expected), getEnumName(actual));

    return false;
}

bool CypherASTComparator::compareTypedPropertySpec(std::string_view field,
                                                   const TypedPropertySpec& expected,
                                                   const TypedPropertySpec& actual) {
    std::vector<std::pair<std::string_view, ValueType>> expectedNames(expected.begin(), expected.end());
    std::vector<std::pair<std::string_view, ValueType>> actualNames(actual.begin(), actual.end());

    std::ranges::sort(expectedNames);
    std::ranges::sort(actualNames);

    if (expectedNames == actualNames) {
        return true;
    }

    std::string expectedText;
    renderTypedNames(expectedNames, expectedText);

    std::string actualText;
    renderTypedNames(actualNames, actualText);

    const PathScope scope(_path, field);
    reportDifference(expectedText, actualText);

    return false;
}

bool CypherASTComparator::compareSize(std::string_view field, size_t expected, size_t actual) {
    if (expected == actual) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(fmt::format("size {}", expected), fmt::format("size {}", actual));

    return false;
}

bool CypherASTComparator::comparePresence(std::string_view field, const void* expected, const void* actual, std::string_view className) {
    const std::string_view expectedClass = expected ? className : NULL_TEXT;
    const std::string_view actualClass = actual ? className : NULL_TEXT;

    if (expectedClass == actualClass) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(expectedClass, actualClass);

    return false;
}

bool CypherASTComparator::compareBool(std::string_view field, bool expected, bool actual) {
    if (expected == actual) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(expected ? "true" : "false", actual ? "true" : "false");

    return false;
}

bool CypherASTComparator::compareInteger(std::string_view field, int64_t expected, int64_t actual) {
    if (expected == actual) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(fmt::format("{}", expected), fmt::format("{}", actual));

    return false;
}

bool CypherASTComparator::compareUnsigned(std::string_view field, uint64_t expected, uint64_t actual) {
    if (expected == actual) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(fmt::format("{}", expected), fmt::format("{}", actual));

    return false;
}

bool CypherASTComparator::compareBound(std::string_view field, int64_t expected, int64_t actual) {
    if (expected == actual) {
        return true;
    }

    std::string expectedText;
    renderBound(expected, expectedText);

    std::string actualText;
    renderBound(actual, actualText);

    const PathScope scope(_path, field);
    reportDifference(expectedText, actualText);

    return false;
}

bool CypherASTComparator::compareDouble(std::string_view field, double expected, double actual) {
    const bool sameBits = std::bit_cast<uint64_t>(expected) == std::bit_cast<uint64_t>(actual);
    if (sameBits) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(fmt::format("{}", expected), fmt::format("{}", actual));

    return false;
}

bool CypherASTComparator::compareFloat(std::string_view field, float expected, float actual) {
    const bool sameBits = std::bit_cast<uint32_t>(expected) == std::bit_cast<uint32_t>(actual);
    if (sameBits) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(fmt::format("{}", expected), fmt::format("{}", actual));

    return false;
}

bool CypherASTComparator::compareChar(std::string_view field, char expected, char actual) {
    if (expected == actual) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(fmt::format("'{}'", expected), fmt::format("'{}'", actual));

    return false;
}

bool CypherASTComparator::compareString(std::string_view field, std::string_view expected, std::string_view actual) {
    if (expected == actual) {
        return true;
    }

    std::string expectedText;
    renderQuoted(expected, expectedText);

    std::string actualText;
    renderQuoted(actual, actualText);

    const PathScope scope(_path, field);
    reportDifference(expectedText, actualText);

    return false;
}

bool CypherASTComparator::compareSymbol(std::string_view field, const Symbol* expected, const Symbol* actual) {
    if (haveSameSymbol(expected, actual)) {
        return true;
    }

    std::string expectedText;
    renderSymbol(expected, expectedText);

    std::string actualText;
    renderSymbol(actual, actualText);

    const PathScope scope(_path, field);
    reportDifference(expectedText, actualText);

    return false;
}

bool CypherASTComparator::compareParameter(std::string_view field, const Parameter* expected, const Parameter* actual) {
    const bool bothAbsent = !expected && !actual;
    const bool sameName = expected && actual && expected->getName() == actual->getName();
    if (bothAbsent || sameName) {
        return true;
    }

    std::string expectedText;
    renderParameter(expected, expectedText);

    std::string actualText;
    renderParameter(actual, actualText);

    const PathScope scope(_path, field);
    reportDifference(expectedText, actualText);

    return false;
}

bool CypherASTComparator::compareQualifiedName(std::string_view field, const QualifiedName* expected, const QualifiedName* actual) {
    if (!comparePresence(field, expected, actual, "QualifiedName")) {
        return false;
    } else if (!expected) {
        return true;
    }

    return compareEach(field, expected->names(), actual->names(), &CypherASTComparator::compareSymbol);
}

bool CypherASTComparator::compareSymbolChain(std::string_view field, const SymbolChain* expected, const SymbolChain* actual) {
    if (!comparePresence(field, expected, actual, "SymbolChain")) {
        return false;
    } else if (!expected) {
        return true;
    }

    return compareEach("names", expected->getVector(), actual->getVector(), &CypherASTComparator::compareSymbol)
           && compareEach("parameters", expected->getParameters(), actual->getParameters(), &CypherASTComparator::compareParameter);
}

bool CypherASTComparator::compareEmbeddingSpec(std::string_view field, const EmbeddingsSpec& expected, const EmbeddingsSpec& actual) {
    std::vector<std::pair<std::string_view, size_t>> expectedEmbeddings(expected.begin(), expected.end());
    std::vector<std::pair<std::string_view, size_t>> actualEmbeddings(actual.begin(), actual.end());

    std::ranges::sort(expectedEmbeddings);
    std::ranges::sort(actualEmbeddings);

    if (expectedEmbeddings == actualEmbeddings) {
        return true;
    }

    std::string expectedText;
    renderEmbeddings(expectedEmbeddings, expectedText);

    std::string actualText;
    renderEmbeddings(actualEmbeddings, actualText);

    const PathScope scope(_path, field);
    reportDifference(expectedText, actualText);

    return false;
}

bool CypherASTComparator::compareExplainRequest(std::string_view field, const ExplainRequest* expected, const ExplainRequest* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "ExplainRequest");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    for (const ExplainStage stage : EXPLAIN_STAGES) {
        if (!compareBool(ExplainRequest::getStageName(stage), expected->isRequested(stage), actual->isRequested(stage))) {
            return false;
        }
    }

    return compareEach("passesBefore", expected->getPassesBefore(), actual->getPassesBefore(), &CypherASTComparator::compareString)
           && compareEach("passesAfter", expected->getPassesAfter(), actual->getPassesAfter(), &CypherASTComparator::compareString);
}

bool CypherASTComparator::compareQuery(std::string_view field, const QueryCommand* expected, const QueryCommand* actual) {
    const PathScope scope(_path, field);

    const std::string_view expectedClass = getClassName(expected);
    const Visit visit = visitNode(expected, actual, expectedClass, getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    appendClassName(expectedClass);

    switch (expected->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            return compareSinglePartQuery(static_cast<const SinglePartQuery*>(expected), static_cast<const SinglePartQuery*>(actual));
        break;
        case QueryCommand::Kind::UNION_QUERY:
            return compareUnionQuery(static_cast<const UnionQuery*>(expected), static_cast<const UnionQuery*>(actual));
        break;
        case QueryCommand::Kind::CONDITIONAL_QUERY:
            return compareConditionalQuery(static_cast<const ConditionalQuery*>(expected), static_cast<const ConditionalQuery*>(actual));
        break;
        case QueryCommand::Kind::LOAD_GRAPH_QUERY:
            return compareLoadGraphQuery(static_cast<const LoadGraphQuery*>(expected), static_cast<const LoadGraphQuery*>(actual));
        break;
        case QueryCommand::Kind::LOAD_JSONL_QUERY:
            return compareLoadJsonlQuery(static_cast<const LoadJsonlQuery*>(expected), static_cast<const LoadJsonlQuery*>(actual));
        break;
        case QueryCommand::Kind::CHANGE_QUERY:
            return compareChangeQuery(static_cast<const ChangeQuery*>(expected), static_cast<const ChangeQuery*>(actual));
        break;
        case QueryCommand::Kind::CREATE_GRAPH_QUERY:
            return compareCreateGraphQuery(static_cast<const CreateGraphQuery*>(expected), static_cast<const CreateGraphQuery*>(actual));
        break;
        case QueryCommand::Kind::LOAD_GML_QUERY:
            return compareLoadGMLQuery(static_cast<const LoadGMLQuery*>(expected), static_cast<const LoadGMLQuery*>(actual));
        break;
        case QueryCommand::Kind::LOAD_PARQUET_QUERY:
            return compareLoadParquetQuery(static_cast<const LoadParquetQuery*>(expected), static_cast<const LoadParquetQuery*>(actual));
        break;
        case QueryCommand::Kind::S3_CONNECT_QUERY:
            return compareS3ConnectQuery(static_cast<const S3ConnectQuery*>(expected), static_cast<const S3ConnectQuery*>(actual));
        break;
        case QueryCommand::Kind::S3_TRANSFER_QUERY:
            return compareS3TransferQuery(static_cast<const S3TransferQuery*>(expected), static_cast<const S3TransferQuery*>(actual));
        break;
        case QueryCommand::Kind::CREATE_VECTOR_INDEX_QUERY:
            return compareCreateVectorIndexQuery(static_cast<const CreateVectorIndexQuery*>(expected), static_cast<const CreateVectorIndexQuery*>(actual));
        break;
        case QueryCommand::Kind::LOAD_VECTOR_QUERY:
            return compareLoadVectorQuery(static_cast<const LoadVectorQuery*>(expected), static_cast<const LoadVectorQuery*>(actual));
        break;
        case QueryCommand::Kind::LOAD_EMBEDDING_QUERY:
            return compareLoadEmbeddingQuery(static_cast<const LoadEmbeddingQuery*>(expected), static_cast<const LoadEmbeddingQuery*>(actual));
        break;
        case QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY:
            return compareDeleteVectorIndexQuery(static_cast<const DeleteVectorIndexQuery*>(expected), static_cast<const DeleteVectorIndexQuery*>(actual));
        break;
        case QueryCommand::Kind::LOAD_COMMIT_QUERY:
            return compareLoadCommitQuery(static_cast<const LoadCommitQuery*>(expected), static_cast<const LoadCommitQuery*>(actual));
        break;
        case QueryCommand::Kind::INSTALL_EXTENSION_QUERY:
            return compareInstallExtensionQuery(static_cast<const InstallExtensionQuery*>(expected), static_cast<const InstallExtensionQuery*>(actual));
        break;
        case QueryCommand::Kind::CREATE_NODE_PROPERTY_INDEX_QUERY:
            return compareCreateNodePropertyIndexQuery(static_cast<const CreateNodePropertyIndexQuery*>(expected),
                                                       static_cast<const CreateNodePropertyIndexQuery*>(actual));
        break;
        case QueryCommand::Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY:
            return compareCreateEdgePropertyIndexQuery(static_cast<const CreateEdgePropertyIndexQuery*>(expected),
                                                       static_cast<const CreateEdgePropertyIndexQuery*>(actual));
        break;
        case QueryCommand::Kind::DROP_INDEX_QUERY:
            return compareDropIndexQuery(static_cast<const DropIndexQuery*>(expected), static_cast<const DropIndexQuery*>(actual));
        break;
        case QueryCommand::Kind::COMMIT_QUERY:
        case QueryCommand::Kind::LIST_GRAPH_QUERY:
        case QueryCommand::Kind::LIST_AVAILABLE_GRAPHS_QUERY:
        case QueryCommand::Kind::SHOW_PROCEDURES_QUERY:
        case QueryCommand::Kind::SHOW_VECTOR_INDEXES_QUERY:
        case QueryCommand::Kind::SHOW_EXTENSIONS_QUERY:
        case QueryCommand::Kind::MERGE_DATAPARTS_QUERY:
            return true;
        break;
    }

    return true;
}

bool CypherASTComparator::compareSinglePartQuery(const SinglePartQuery* expected, const SinglePartQuery* actual) {
    return compareStmtContainer("stmts", expected->getStmts(), actual->getStmts())
           && compareSubclassStmt("returnStmt", expected->getReturnStmt(), actual->getReturnStmt());
}

bool CypherASTComparator::compareUnionQuery(const UnionQuery* expected, const UnionQuery* actual) {
    return compareEach("branches", expected->branches(), actual->branches(), &CypherASTComparator::compareUnionBranch);
}

bool CypherASTComparator::compareUnionBranch(std::string_view field, const UnionQuery::Branch& expected, const UnionQuery::Branch& actual) {
    const PathScope scope(_path, field);

    return compareBool("all", expected._all, actual._all)
           && compareQuery("query", expected._query, actual._query);
}

bool CypherASTComparator::compareConditionalQuery(const ConditionalQuery* expected, const ConditionalQuery* actual) {
    return compareEach("branches", expected->branches(), actual->branches(), &CypherASTComparator::compareConditionalBranch);
}

bool CypherASTComparator::compareConditionalBranch(std::string_view field,
                                                   const ConditionalQuery::Branch& expected,
                                                   const ConditionalQuery::Branch& actual) {
    const PathScope scope(_path, field);

    return compareExpr("condition", expected._condition, actual._condition)
           && compareQuery("query", expected._query, actual._query);
}

bool CypherASTComparator::compareLoadGraphQuery(const LoadGraphQuery* expected, const LoadGraphQuery* actual) {
    return compareString("graphName", expected->getGraphName(), actual->getGraphName());
}

bool CypherASTComparator::compareLoadJsonlQuery(const LoadJsonlQuery* expected, const LoadJsonlQuery* actual) {
    return compareString("filePath", expected->getFilePath().get(), actual->getFilePath().get())
           && compareString("graphName", expected->getGraphName(), actual->getGraphName())
           && compareEmbeddingSpec("embeddingSpecs", expected->getEmbeddingSpecs(), actual->getEmbeddingSpecs())
           && compareTypedPropertySpec("typedPropertySpec", expected->getTypedPropertySpec(), actual->getTypedPropertySpec());
}

bool CypherASTComparator::compareChangeQuery(const ChangeQuery* expected, const ChangeQuery* actual) {
    return compareEnum("op", expected->getOp(), actual->getOp());
}

bool CypherASTComparator::compareCreateGraphQuery(const CreateGraphQuery* expected, const CreateGraphQuery* actual) {
    return compareString("graphName", expected->getGraphName(), actual->getGraphName());
}

bool CypherASTComparator::compareLoadGMLQuery(const LoadGMLQuery* expected, const LoadGMLQuery* actual) {
    return compareString("filePath", expected->getFilePath().get(), actual->getFilePath().get())
           && compareString("graphName", expected->getGraphName(), actual->getGraphName());
}

bool CypherASTComparator::compareLoadParquetQuery(const LoadParquetQuery* expected, const LoadParquetQuery* actual) {
    return compareString("filePath", expected->getFilePath().get(), actual->getFilePath().get())
           && compareString("graphName", expected->getGraphName(), actual->getGraphName())
           && compareTypedPropertySpec("typedPropertySpec", expected->getTypedPropertySpec(), actual->getTypedPropertySpec());
}

bool CypherASTComparator::compareS3ConnectQuery(const S3ConnectQuery* expected, const S3ConnectQuery* actual) {
    return compareString("accessId", expected->getAccessId(), actual->getAccessId())
           && compareString("secretKey", expected->getSecretKey(), actual->getSecretKey())
           && compareString("region", expected->getRegion(), actual->getRegion());
}

bool CypherASTComparator::compareS3TransferQuery(const S3TransferQuery* expected, const S3TransferQuery* actual) {
    return compareEnum("direction", expected->getDirection(), actual->getDirection())
           && compareString("s3Url", expected->getS3Url(), actual->getS3Url())
           && compareString("localPath", expected->getLocalPath(), actual->getLocalPath());
}

bool CypherASTComparator::compareCreateVectorIndexQuery(const CreateVectorIndexQuery* expected, const CreateVectorIndexQuery* actual) {
    return compareString("indexName", expected->getIndexName(), actual->getIndexName())
           && compareInteger("dimension", expected->getDimension(), actual->getDimension())
           && compareEnum("metric", expected->getMetric(), actual->getMetric())
           && compareEnum("indexType", expected->getIndexType(), actual->getIndexType());
}

bool CypherASTComparator::compareLoadVectorQuery(const LoadVectorQuery* expected, const LoadVectorQuery* actual) {
    return compareString("filePath", expected->getFilePath(), actual->getFilePath())
           && compareString("indexName", expected->getIndexName(), actual->getIndexName());
}

bool CypherASTComparator::compareLoadEmbeddingQuery(const LoadEmbeddingQuery* expected, const LoadEmbeddingQuery* actual) {
    return compareString("filePath", expected->getFilePath(), actual->getFilePath())
           && compareString("propertyName", expected->getPropertyName(), actual->getPropertyName());
}

bool CypherASTComparator::compareDeleteVectorIndexQuery(const DeleteVectorIndexQuery* expected, const DeleteVectorIndexQuery* actual) {
    return compareString("indexName", expected->getIndexName(), actual->getIndexName());
}

bool CypherASTComparator::compareLoadCommitQuery(const LoadCommitQuery* expected, const LoadCommitQuery* actual) {
    return compareString("hashStr", expected->getHashStr(), actual->getHashStr());
}

bool CypherASTComparator::compareInstallExtensionQuery(const InstallExtensionQuery* expected, const InstallExtensionQuery* actual) {
    return compareString("extensionName", expected->getExtensionName(), actual->getExtensionName());
}

bool CypherASTComparator::compareCreateNodePropertyIndexQuery(const CreateNodePropertyIndexQuery* expected,
                                                              const CreateNodePropertyIndexQuery* actual) {
    return compareString("indexName", expected->indexName(), actual->indexName())
           && compareSubclassEntity("nodePattern", expected->nodePattern(), actual->nodePattern())
           && compareSubclassExpr("propertyExpr", expected->propertyExpr(), actual->propertyExpr());
}

bool CypherASTComparator::compareCreateEdgePropertyIndexQuery(const CreateEdgePropertyIndexQuery* expected,
                                                              const CreateEdgePropertyIndexQuery* actual) {
    return compareString("indexName", expected->indexName(), actual->indexName())
           && compareSubclassEntity("edgePattern", expected->edgePattern(), actual->edgePattern())
           && compareSubclassExpr("propertyExpr", expected->propertyExpr(), actual->propertyExpr());
}

bool CypherASTComparator::compareDropIndexQuery(const DropIndexQuery* expected, const DropIndexQuery* actual) {
    return compareString("indexName", expected->indexName(), actual->indexName());
}

bool CypherASTComparator::compareStmt(std::string_view field, const Stmt* expected, const Stmt* actual) {
    const PathScope scope(_path, field);

    const std::string_view expectedClass = getClassName(expected);
    const Visit visit = visitNode(expected, actual, expectedClass, getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    appendClassName(expectedClass);

    return compareStmtContents(expected, actual);
}

bool CypherASTComparator::compareSubclassStmt(std::string_view field, const Stmt* expected, const Stmt* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, getClassName(expected), getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareStmtContents(expected, actual);
}

bool CypherASTComparator::compareStmtContents(const Stmt* expected, const Stmt* actual) {
    switch (expected->getKind()) {
        case Stmt::Kind::MATCH:
            return compareMatchStmt(static_cast<const MatchStmt*>(expected), static_cast<const MatchStmt*>(actual));
        break;
        case Stmt::Kind::CALL:
            return compareCallStmt(static_cast<const CallStmt*>(expected), static_cast<const CallStmt*>(actual));
        break;
        case Stmt::Kind::CREATE:
            return compareCreateStmt(static_cast<const CreateStmt*>(expected), static_cast<const CreateStmt*>(actual));
        break;
        case Stmt::Kind::MERGE:
            return compareMergeStmt(static_cast<const MergeStmt*>(expected), static_cast<const MergeStmt*>(actual));
        break;
        case Stmt::Kind::SET:
            return compareSetStmt(static_cast<const SetStmt*>(expected), static_cast<const SetStmt*>(actual));
        break;
        case Stmt::Kind::REMOVE:
            return compareRemoveStmt(static_cast<const RemoveStmt*>(expected), static_cast<const RemoveStmt*>(actual));
        break;
        case Stmt::Kind::DELETE:
            return compareDeleteStmt(static_cast<const DeleteStmt*>(expected), static_cast<const DeleteStmt*>(actual));
        break;
        case Stmt::Kind::RETURN:
            return compareReturnStmt(static_cast<const ReturnStmt*>(expected), static_cast<const ReturnStmt*>(actual));
        break;
        case Stmt::Kind::SHORTESTPATH:
            return compareShortestPathStmt(static_cast<const ShortestPathStmt*>(expected), static_cast<const ShortestPathStmt*>(actual));
        break;
        case Stmt::Kind::LOAD_CSV:
            return compareLoadCSVStmt(static_cast<const LoadCSVStmt*>(expected), static_cast<const LoadCSVStmt*>(actual));
        break;
        case Stmt::Kind::VECTOR_SEARCH:
            return compareVectorSearchStmt(static_cast<const VectorSearchStmt*>(expected), static_cast<const VectorSearchStmt*>(actual));
        break;
        case Stmt::Kind::UNWIND:
            return compareUnwindStmt(static_cast<const UnwindStmt*>(expected), static_cast<const UnwindStmt*>(actual));
        break;
        case Stmt::Kind::WITH:
            return compareWithStmt(static_cast<const WithStmt*>(expected), static_cast<const WithStmt*>(actual));
        break;
        case Stmt::Kind::CALL_SUBQUERY:
            return compareCallSubqueryStmt(static_cast<const CallSubqueryStmt*>(expected), static_cast<const CallSubqueryStmt*>(actual));
        break;
        case Stmt::Kind::NEXT_SEGMENT:
            return compareNextSegmentStmt(static_cast<const NextSegmentStmt*>(expected), static_cast<const NextSegmentStmt*>(actual));
        break;
    }

    return true;
}

bool CypherASTComparator::compareStmtContainer(std::string_view field, const StmtContainer* expected, const StmtContainer* actual) {
    const Visit visit = visitField(field, expected, actual, "StmtContainer");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareEach(field, expected->stmts(), actual->stmts(), &CypherASTComparator::compareStmt);
}

bool CypherASTComparator::compareMatchStmt(const MatchStmt* expected, const MatchStmt* actual) {
    return compareBool("optional", expected->isOptional(), actual->isOptional())
           && comparePattern("pattern", expected->getPattern(), actual->getPattern())
           && compareOrderBy("orderBy", expected->getOrderBy(), actual->getOrderBy())
           && compareSkip("skip", expected->getSkip(), actual->getSkip())
           && compareLimit("limit", expected->getLimit(), actual->getLimit());
}

bool CypherASTComparator::compareCallStmt(const CallStmt* expected, const CallStmt* actual) {
    return compareBool("optional", expected->isOptional(), actual->isOptional())
           && compareBool("standaloneCall", expected->isStandaloneCall(), actual->isStandaloneCall())
           && compareSubclassExpr("func", expected->getFunc(), actual->getFunc())
           && compareYieldClause("yield", expected->getYield(), actual->getYield());
}

bool CypherASTComparator::compareCallSubqueryStmt(const CallSubqueryStmt* expected, const CallSubqueryStmt* actual) {
    return compareBool("optional", expected->isOptional(), actual->isOptional())
           && compareBool("scopeClause", expected->hasScopeClause(), actual->hasScopeClause())
           && compareBool("importsEverything", expected->importsEverything(), actual->importsEverything())
           && compareEach("imports", expected->getImports(), actual->getImports(), &CypherASTComparator::compareSymbol)
           && compareQuery("body", expected->getBody(), actual->getBody());
}

bool CypherASTComparator::compareCreateStmt(const CreateStmt* expected, const CreateStmt* actual) {
    return comparePattern("pattern", expected->getPattern(), actual->getPattern());
}

bool CypherASTComparator::compareMergeStmt(const MergeStmt* expected, const MergeStmt* actual) {
    return comparePattern("pattern", expected->getPattern(), actual->getPattern())
           && compareSubclassStmt("onCreate", expected->getOnCreate(), actual->getOnCreate())
           && compareSubclassStmt("onMatch", expected->getOnMatch(), actual->getOnMatch());
}

bool CypherASTComparator::compareSetStmt(const SetStmt* expected, const SetStmt* actual) {
    return compareEach("items", expected->getItems(), actual->getItems(), &CypherASTComparator::compareSetItem);
}

bool CypherASTComparator::compareSetItem(std::string_view field, const SetItem* expected, const SetItem* actual) {
    const PathScope scope(_path, field);

    const std::string_view expectedClass = getClassName(expected);
    const Visit visit = visitNode(expected, actual, expectedClass, getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    appendClassName(expectedClass);

    const SetItem::Variant& expectedItem = expected->item();
    const SetItem::Variant& actualItem = actual->item();

    if (const SetItem::PropertyExprAssign* expectedAssign = std::get_if<SetItem::PropertyExprAssign>(&expectedItem)) {
        const SetItem::PropertyExprAssign& actualAssign = std::get<SetItem::PropertyExprAssign>(actualItem);

        return compareSubclassExpr("property", expectedAssign->_propTypeExpr, actualAssign._propTypeExpr)
               && compareExpr("value", expectedAssign->_propValueExpr, actualAssign._propValueExpr);
    } else if (const SetItem::EntityPropertiesAssign* expectedProperties = std::get_if<SetItem::EntityPropertiesAssign>(&expectedItem)) {
        const SetItem::EntityPropertiesAssign& actualProperties = std::get<SetItem::EntityPropertiesAssign>(actualItem);

        return compareExpr("entity", expectedProperties->_entity, actualProperties._entity)
               && compareExpr("value", expectedProperties->_value, actualProperties._value)
               && compareBool("replaces", expectedProperties->_replaces, actualProperties._replaces);
    }

    const SetItem::SymbolEntityTypes& expectedTypes = std::get<SetItem::SymbolEntityTypes>(expectedItem);
    const SetItem::SymbolEntityTypes& actualTypes = std::get<SetItem::SymbolEntityTypes>(actualItem);

    return compareSubclassExpr("value", expectedTypes._value, actualTypes._value);
}

bool CypherASTComparator::compareRemoveStmt(const RemoveStmt* expected, const RemoveStmt* actual) {
    return compareEach("properties", expected->getProperties(), actual->getProperties(), &CypherASTComparator::compareSubclassExpr);
}

bool CypherASTComparator::compareDeleteStmt(const DeleteStmt* expected, const DeleteStmt* actual) {
    return compareBool("detaching", expected->isDetaching(), actual->isDetaching())
           && compareExprChain("expressions", expected->getExpressions(), actual->getExpressions());
}

bool CypherASTComparator::compareReturnStmt(const ReturnStmt* expected, const ReturnStmt* actual) {
    return compareProjection("projection", expected->getProjection(), actual->getProjection());
}

bool CypherASTComparator::compareShortestPathStmt(const ShortestPathStmt* expected, const ShortestPathStmt* actual) {
    return compareSymbol("source", expected->getSource(), actual->getSource())
           && compareSymbol("target", expected->getTarget(), actual->getTarget())
           && compareSymbol("edgeProperty", expected->getEdgeProperty(), actual->getEdgeProperty())
           && compareSymbol("distVar", expected->getDistVar(), actual->getDistVar())
           && compareSymbol("pathVar", expected->getPathVar(), actual->getPathVar());
}

bool CypherASTComparator::compareLoadCSVStmt(const LoadCSVStmt* expected, const LoadCSVStmt* actual) {
    return compareString("filePath", expected->getFilePath().get(), actual->getFilePath().get())
           && compareSymbol("alias", expected->getAlias(), actual->getAlias())
           && compareBool("headers", expected->hasHeaders(), actual->hasHeaders())
           && compareBool("skipOnError", expected->skipOnError(), actual->skipOnError());
}

bool CypherASTComparator::compareVectorSearchStmt(const VectorSearchStmt* expected, const VectorSearchStmt* actual) {
    return compareString("indexName", expected->getIndexName(), actual->getIndexName())
           && compareUnsigned("k", expected->getK(), actual->getK())
           && compareLiteral("queryVector", expected->getQueryVector(), actual->getQueryVector())
           && compareYieldClause("yield", expected->getYield(), actual->getYield());
}

bool CypherASTComparator::compareUnwindStmt(const UnwindStmt* expected, const UnwindStmt* actual) {
    return compareExpr("arg", expected->arg(), actual->arg())
           && compareSymbol("symbol", expected->symbol(), actual->symbol());
}

bool CypherASTComparator::compareWithStmt(const WithStmt* expected, const WithStmt* actual) {
    return compareProjection("projection", expected->getProjection(), actual->getProjection())
           && compareWhereClause("where", expected->getWhere(), actual->getWhere())
           && compareBool("segmentReturn", expected->isSegmentReturn(), actual->isSegmentReturn());
}

bool CypherASTComparator::compareNextSegmentStmt(const NextSegmentStmt* expected, const NextSegmentStmt* actual) {
    return compareSubclassStmt("body", expected->getBody(), actual->getBody());
}

bool CypherASTComparator::compareProjection(std::string_view field, const Projection* expected, const Projection* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "Projection");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareBool("distinct", expected->isDistinct(), actual->isDistinct())
           && compareBool("returningAll", expected->isReturningAll(), actual->isReturningAll())
           && compareEach("items", expected->items(), actual->items(), &CypherASTComparator::compareProjectionItem)
           && compareOrderBy("orderBy", expected->getOrderBy(), actual->getOrderBy())
           && compareSkip("skip", expected->getSkip(), actual->getSkip())
           && compareLimit("limit", expected->getLimit(), actual->getLimit());
}

bool CypherASTComparator::compareProjectionItem(std::string_view field,
                                                const Projection::ReturnItem& expected,
                                                const Projection::ReturnItem& actual) {
    Expr* const* expectedExpr = std::get_if<Expr*>(&expected);
    Expr* const* actualExpr = std::get_if<Expr*>(&actual);

    if (expectedExpr && actualExpr) {
        return compareExpr(field, *expectedExpr, *actualExpr);
    } else if (!expectedExpr && !actualExpr) {
        return true;
    }

    const PathScope scope(_path, field);
    reportDifference(getClassName(expected), getClassName(actual));

    return false;
}

bool CypherASTComparator::compareOrderBy(std::string_view field, const OrderBy* expected, const OrderBy* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "OrderBy");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareEach("items", expected->getItems(), actual->getItems(), &CypherASTComparator::compareOrderByItem);
}

bool CypherASTComparator::compareOrderByItem(std::string_view field, const OrderByItem* expected, const OrderByItem* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "OrderByItem");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareExpr("expr", expected->getExpr(), actual->getExpr())
           && compareEnum("type", expected->getType(), actual->getType());
}

bool CypherASTComparator::compareSkip(std::string_view field, const Skip* expected, const Skip* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "Skip");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareExpr("expr", expected->getExpr(), actual->getExpr());
}

bool CypherASTComparator::compareLimit(std::string_view field, const Limit* expected, const Limit* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "Limit");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareExpr("expr", expected->getExpr(), actual->getExpr());
}

bool CypherASTComparator::compareWhereClause(std::string_view field, const WhereClause* expected, const WhereClause* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "WhereClause");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareExpr("expr", expected->getExpr(), actual->getExpr());
}

bool CypherASTComparator::compareYieldClause(std::string_view field, const YieldClause* expected, const YieldClause* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "YieldClause");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    const YieldItems* expectedItems = expected->getItems();
    const YieldItems* actualItems = actual->getItems();

    const Visit itemsVisit = visitField("items", expectedItems, actualItems, "YieldItems");
    if (itemsVisit != Visit::PENDING) {
        return itemsVisit == Visit::EQUAL;
    }

    return compareEach("items", expectedItems->getItems(), actualItems->getItems(), &CypherASTComparator::compareSubclassExpr)
           && compareWhereClause("where", expectedItems->getWhereClause(), actualItems->getWhereClause());
}

bool CypherASTComparator::comparePattern(std::string_view field, const Pattern* expected, const Pattern* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "Pattern");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareEach("elements", expected->elements(), actual->elements(), &CypherASTComparator::comparePatternElement)
           && compareWhereClause("where", expected->getWhere(), actual->getWhere());
}

bool CypherASTComparator::comparePatternElement(std::string_view field, const PatternElement* expected, const PatternElement* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "PatternElement");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareSymbol("pathSymbol", expected->getPathSymbol(), actual->getPathSymbol())
           && compareEach("entities", expected->getEntities(), actual->getEntities(), &CypherASTComparator::compareEntity);
}

bool CypherASTComparator::compareEntity(std::string_view field, const EntityPattern* expected, const EntityPattern* actual) {
    const PathScope scope(_path, field);

    const std::string_view expectedClass = getClassName(expected);
    const Visit visit = visitNode(expected, actual, expectedClass, getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    appendClassName(expectedClass);

    return compareEntityContents(expected, actual);
}

bool CypherASTComparator::compareSubclassEntity(std::string_view field, const EntityPattern* expected, const EntityPattern* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, getClassName(expected), getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareEntityContents(expected, actual);
}

bool CypherASTComparator::compareEntityContents(const EntityPattern* expected, const EntityPattern* actual) {
    const NodePattern* expectedNode = dynamic_cast<const NodePattern*>(expected);
    if (expectedNode) {
        return compareNodePattern(expectedNode, static_cast<const NodePattern*>(actual));
    }

    return compareEdgePattern(static_cast<const EdgePattern*>(expected), static_cast<const EdgePattern*>(actual));
}

bool CypherASTComparator::compareNodePattern(const NodePattern* expected, const NodePattern* actual) {
    return compareSymbol("symbol", expected->getSymbol(), actual->getSymbol())
           && compareLabelExpr("labelExpr", expected->getLabelExpr(), actual->getLabelExpr())
           && compareLiteral("properties", expected->getProperties(), actual->getProperties())
           && compareWhereClause("where", expected->getWhere(), actual->getWhere());
}

bool CypherASTComparator::compareEdgePattern(const EdgePattern* expected, const EdgePattern* actual) {
    return compareEnum("direction", expected->getDirection(), actual->getDirection())
           && compareSymbol("symbol", expected->getSymbol(), actual->getSymbol())
           && compareLabelExpr("labelExpr", expected->getLabelExpr(), actual->getLabelExpr())
           && compareQuantifiedPath("quantifiedPath", expected->getQuantifiedPath(), actual->getQuantifiedPath())
           && compareLiteral("properties", expected->getProperties(), actual->getProperties())
           && compareWhereClause("where", expected->getWhere(), actual->getWhere())
           && compareHops(expected, actual)
           && compareWhereClause("hopWhere", expected->getHopWhere(), actual->getHopWhere());
}

bool CypherASTComparator::compareHops(const EdgePattern* expected, const EdgePattern* actual) {
    const size_t hopCount = expected->getHopCount();
    if (!compareSize("hops", hopCount, actual->getHopCount())) {
        return false;
    }

    std::string label;
    for (size_t hop = 0; hop < hopCount; hop++) {
        formatIndexedField("hops", hop, label);

        if (!compareSubclassEntity(label, expected->getHop(hop), actual->getHop(hop))) {
            return false;
        }
    }

    const size_t hopNodeCount = expected->getHopNodeCount();
    if (!compareSize("hopNodes", hopNodeCount, actual->getHopNodeCount())) {
        return false;
    }

    for (size_t position = 0; position < hopNodeCount; position++) {
        formatIndexedField("hopNodes", position, label);

        if (!compareSubclassEntity(label, expected->getHopNode(position), actual->getHopNode(position))) {
            return false;
        }
    }

    return true;
}

bool CypherASTComparator::compareQuantifiedPath(std::string_view field, const QuantifiedPath* expected, const QuantifiedPath* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, "QuantifiedPath");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareBound("lhs", expected->getLhs(), actual->getLhs())
           && compareBound("rhs", expected->getRhs(), actual->getRhs());
}

bool CypherASTComparator::compareLabelExpr(std::string_view field, const LabelExpr* expected, const LabelExpr* actual) {
    const PathScope scope(_path, field);

    const std::string_view expectedClass = getClassName(expected);
    const Visit visit = visitNode(expected, actual, expectedClass, getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    appendClassName(expectedClass);

    return compareSymbolChain("labels", expected->getLabels(), actual->getLabels())
           && compareEach("operands", expected->getOperands(), actual->getOperands(), &CypherASTComparator::compareLabelExpr);
}

bool CypherASTComparator::compareExpr(std::string_view field, const Expr* expected, const Expr* actual) {
    const PathScope scope(_path, field);

    const std::string_view expectedClass = getClassName(expected);
    const Visit visit = visitNode(expected, actual, expectedClass, getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    appendClassName(expectedClass);

    return compareExprContents(expected, actual);
}

bool CypherASTComparator::compareSubclassExpr(std::string_view field, const Expr* expected, const Expr* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, getClassName(expected), getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareExprContents(expected, actual);
}

bool CypherASTComparator::compareExprContents(const Expr* expected, const Expr* actual) {
    const bool sameAttributes = compareBool("parenthesized", expected->isParenthesized(), actual->isParenthesized())
                                && compareString("name", expected->getName(), actual->getName());
    if (!sameAttributes) {
        return false;
    }

    switch (expected->getKind()) {
        case Expr::Kind::BINARY:
            return compareBinaryExpr(static_cast<const BinaryExpr*>(expected), static_cast<const BinaryExpr*>(actual));
        break;
        case Expr::Kind::LOGICAL:
            return compareLogicalExpr(static_cast<const LogicalExpr*>(expected), static_cast<const LogicalExpr*>(actual));
        break;
        case Expr::Kind::UNARY:
            return compareUnaryExpr(static_cast<const UnaryExpr*>(expected), static_cast<const UnaryExpr*>(actual));
        break;
        case Expr::Kind::STRING:
            return compareStringExpr(static_cast<const StringExpr*>(expected), static_cast<const StringExpr*>(actual));
        break;
        case Expr::Kind::ENTITY_TYPES:
            return compareEntityTypeExpr(static_cast<const EntityTypeExpr*>(expected), static_cast<const EntityTypeExpr*>(actual));
        break;
        case Expr::Kind::PROPERTY:
            return comparePropertyExpr(static_cast<const PropertyExpr*>(expected), static_cast<const PropertyExpr*>(actual));
        break;
        case Expr::Kind::PROPERTY_LOOKUP:
            return comparePropertyLookupExpr(static_cast<const PropertyLookupExpr*>(expected), static_cast<const PropertyLookupExpr*>(actual));
        break;
        case Expr::Kind::SYMBOL:
            return compareSymbolExpr(static_cast<const SymbolExpr*>(expected), static_cast<const SymbolExpr*>(actual));
        break;
        case Expr::Kind::LITERAL:
            return compareLiteralExpr(static_cast<const LiteralExpr*>(expected), static_cast<const LiteralExpr*>(actual));
        break;
        case Expr::Kind::FUNCTION_INVOCATION:
            return compareFunctionInvocationExpr(static_cast<const FunctionInvocationExpr*>(expected),
                                                 static_cast<const FunctionInvocationExpr*>(actual));
        break;
        case Expr::Kind::INDEX:
            return compareIndexExpr(static_cast<const IndexExpr*>(expected), static_cast<const IndexExpr*>(actual));
        break;
        case Expr::Kind::LIST:
            return compareListExpr(static_cast<const ListExpr*>(expected), static_cast<const ListExpr*>(actual));
        break;
        case Expr::Kind::LIST_SLICE:
            return compareListSliceExpr(static_cast<const ListSliceExpr*>(expected), static_cast<const ListSliceExpr*>(actual));
        break;
        case Expr::Kind::LIST_COMPREHENSION:
            return compareListComprehensionExpr(static_cast<const ListComprehensionExpr*>(expected),
                                                static_cast<const ListComprehensionExpr*>(actual));
        break;
        case Expr::Kind::PATTERN_COMPREHENSION:
            return comparePatternComprehensionExpr(static_cast<const PatternComprehensionExpr*>(expected),
                                                   static_cast<const PatternComprehensionExpr*>(actual));
        break;
        case Expr::Kind::LIST_PREDICATE:
            return compareListPredicateExpr(static_cast<const ListPredicateExpr*>(expected), static_cast<const ListPredicateExpr*>(actual));
        break;
        case Expr::Kind::REDUCE:
            return compareReduceExpr(static_cast<const ReduceExpr*>(expected), static_cast<const ReduceExpr*>(actual));
        break;
        case Expr::Kind::ALL_REDUCE:
            return compareAllReduceExpr(static_cast<const AllReduceExpr*>(expected), static_cast<const AllReduceExpr*>(actual));
        break;
        case Expr::Kind::MAP_PROJECTION:
            return compareMapProjectionExpr(static_cast<const MapProjectionExpr*>(expected), static_cast<const MapProjectionExpr*>(actual));
        break;
        case Expr::Kind::CASE:
            return compareCaseExpr(static_cast<const CaseExpr*>(expected), static_cast<const CaseExpr*>(actual));
        break;
        case Expr::Kind::EXISTS:
            return compareExistsExpr(static_cast<const ExistsExpr*>(expected), static_cast<const ExistsExpr*>(actual));
        break;
        case Expr::Kind::COUNT_SUBQUERY:
            return compareCountSubqueryExpr(static_cast<const CountSubqueryExpr*>(expected), static_cast<const CountSubqueryExpr*>(actual));
        break;
        case Expr::Kind::COLLECT_SUBQUERY:
            return compareCollectSubqueryExpr(static_cast<const CollectSubqueryExpr*>(expected), static_cast<const CollectSubqueryExpr*>(actual));
        break;
        case Expr::Kind::PARAMETER:
            return compareParameterExpr(static_cast<const ParameterExpr*>(expected), static_cast<const ParameterExpr*>(actual));
        break;
        case Expr::Kind::_SIZE:
        break;
    }

    return true;
}

bool CypherASTComparator::compareExprChain(std::string_view field, const ExprChain* expected, const ExprChain* actual) {
    const Visit visit = visitField(field, expected, actual, "ExprChain");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareEach(field, expected->getExprs(), actual->getExprs(), &CypherASTComparator::compareExpr);
}

bool CypherASTComparator::compareBinaryExpr(const BinaryExpr* expected, const BinaryExpr* actual) {
    return compareEnum("operator", expected->getOperator(), actual->getOperator())
           && compareExpr("lhs", expected->getLHS(), actual->getLHS())
           && compareExpr("rhs", expected->getRHS(), actual->getRHS());
}

bool CypherASTComparator::compareLogicalExpr(const LogicalExpr* expected, const LogicalExpr* actual) {
    return compareEnum("operator", expected->getOperator(), actual->getOperator())
           && compareBool("comparisonChain", expected->isComparisonChain(), actual->isComparisonChain())
           && compareEach("operands", expected->getOperands(), actual->getOperands(), &CypherASTComparator::compareExpr);
}

bool CypherASTComparator::compareUnaryExpr(const UnaryExpr* expected, const UnaryExpr* actual) {
    return compareEnum("operator", expected->getOperator(), actual->getOperator())
           && compareExpr("subExpr", expected->getSubExpr(), actual->getSubExpr());
}

bool CypherASTComparator::compareStringExpr(const StringExpr* expected, const StringExpr* actual) {
    return compareEnum("operator", expected->getStringOperator(), actual->getStringOperator())
           && compareExpr("lhs", expected->getLHS(), actual->getLHS())
           && compareExpr("rhs", expected->getRHS(), actual->getRHS());
}

bool CypherASTComparator::compareEntityTypeExpr(const EntityTypeExpr* expected, const EntityTypeExpr* actual) {
    return compareSymbol("symbol", expected->getSymbol(), actual->getSymbol())
           && compareExpr("operand", expected->getOperand(), actual->getOperand())
           && compareLabelExpr("labelExpr", expected->getLabelExpr(), actual->getLabelExpr());
}

bool CypherASTComparator::comparePropertyExpr(const PropertyExpr* expected, const PropertyExpr* actual) {
    return compareQualifiedName("fullName", expected->getFullName(), actual->getFullName());
}

bool CypherASTComparator::comparePropertyLookupExpr(const PropertyLookupExpr* expected, const PropertyLookupExpr* actual) {
    return compareExpr("base", expected->getBase(), actual->getBase())
           && compareString("propName", expected->getPropName(), actual->getPropName());
}

bool CypherASTComparator::compareSymbolExpr(const SymbolExpr* expected, const SymbolExpr* actual) {
    return compareSymbol("symbol", expected->getSymbol(), actual->getSymbol());
}

bool CypherASTComparator::compareLiteralExpr(const LiteralExpr* expected, const LiteralExpr* actual) {
    const Literal* expectedLiteral = expected->getLiteral();
    const Literal* actualLiteral = actual->getLiteral();

    const Visit visit = visitField("literal", expectedLiteral, actualLiteral, getClassName(expectedLiteral), getClassName(actualLiteral));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareLiteralContents(expectedLiteral, actualLiteral);
}

bool CypherASTComparator::compareFunctionInvocationExpr(const FunctionInvocationExpr* expected, const FunctionInvocationExpr* actual) {
    const FunctionInvocation* expectedInvocation = expected->getFunctionInvocation();
    const FunctionInvocation* actualInvocation = actual->getFunctionInvocation();

    const Visit visit = visitField("invocation", expectedInvocation, actualInvocation, "FunctionInvocation");
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareQualifiedName("name", expectedInvocation->getName(), actualInvocation->getName())
           && compareBool("distinct", expectedInvocation->isDistinct(), actualInvocation->isDistinct())
           && compareExprChain("arguments", expectedInvocation->getArguments(), actualInvocation->getArguments());
}

bool CypherASTComparator::compareIndexExpr(const IndexExpr* expected, const IndexExpr* actual) {
    return compareExpr("base", expected->getBase(), actual->getBase())
           && compareExpr("indexExpr", expected->getIndexExpr(), actual->getIndexExpr());
}

bool CypherASTComparator::compareListExpr(const ListExpr* expected, const ListExpr* actual) {
    return compareEach("elements", expected->getElements(), actual->getElements(), &CypherASTComparator::compareExpr);
}

bool CypherASTComparator::compareListSliceExpr(const ListSliceExpr* expected, const ListSliceExpr* actual) {
    return compareExpr("base", expected->getBase(), actual->getBase())
           && compareExpr("from", expected->getFrom(), actual->getFrom())
           && compareExpr("to", expected->getTo(), actual->getTo());
}

bool CypherASTComparator::compareListComprehensionExpr(const ListComprehensionExpr* expected, const ListComprehensionExpr* actual) {
    return compareSymbol("symbol", expected->getSymbol(), actual->getSymbol())
           && compareExpr("source", expected->getSource(), actual->getSource())
           && compareExpr("predicate", expected->getPredicate(), actual->getPredicate())
           && compareExpr("projection", expected->getProjection(), actual->getProjection());
}

bool CypherASTComparator::comparePatternComprehensionExpr(const PatternComprehensionExpr* expected, const PatternComprehensionExpr* actual) {
    return compareSubclassStmt("match", expected->getMatch(), actual->getMatch())
           && compareExpr("projection", expected->getProjection(), actual->getProjection());
}

bool CypherASTComparator::compareListPredicateExpr(const ListPredicateExpr* expected, const ListPredicateExpr* actual) {
    return compareEnum("quantifier", expected->getQuantifier(), actual->getQuantifier())
           && compareSubclassExpr("comprehension", expected->getComprehension(), actual->getComprehension());
}

bool CypherASTComparator::compareReduceExpr(const ReduceExpr* expected, const ReduceExpr* actual) {
    return compareSymbol("accumulatorSymbol", expected->getAccumulatorSymbol(), actual->getAccumulatorSymbol())
           && compareExpr("initialValue", expected->getInitialValue(), actual->getInitialValue())
           && compareSymbol("itemSymbol", expected->getItemSymbol(), actual->getItemSymbol())
           && compareExpr("source", expected->getSource(), actual->getSource())
           && compareExpr("expression", expected->getExpression(), actual->getExpression());
}

bool CypherASTComparator::compareAllReduceExpr(const AllReduceExpr* expected, const AllReduceExpr* actual) {
    return compareSymbol("accumulatorSymbol", expected->getAccumulatorSymbol(), actual->getAccumulatorSymbol())
           && compareExpr("initialValue", expected->getInitialValue(), actual->getInitialValue())
           && compareSymbol("itemSymbol", expected->getItemSymbol(), actual->getItemSymbol())
           && compareExpr("source", expected->getSource(), actual->getSource())
           && compareExpr("expression", expected->getExpression(), actual->getExpression())
           && compareExpr("predicate", expected->getPredicate(), actual->getPredicate());
}

bool CypherASTComparator::compareMapProjectionExpr(const MapProjectionExpr* expected, const MapProjectionExpr* actual) {
    return compareSubclassExpr("base", expected->getBase(), actual->getBase())
           && compareEach("items", expected->getItems(), actual->getItems(), &CypherASTComparator::compareMapProjectionItem);
}

bool CypherASTComparator::compareMapProjectionItem(std::string_view field,
                                                   const MapProjectionExpr::Item& expected,
                                                   const MapProjectionExpr::Item& actual) {
    const PathScope scope(_path, field);

    return compareEnum("kind", expected._kind, actual._kind)
           && compareSymbol("key", expected._key, actual._key)
           && compareExpr("value", expected._value, actual._value);
}

bool CypherASTComparator::compareCaseExpr(const CaseExpr* expected, const CaseExpr* actual) {
    return compareExpr("subject", expected->getSubject(), actual->getSubject())
           && compareEach("branches", expected->getBranches(), actual->getBranches(), &CypherASTComparator::compareCaseBranch)
           && compareExpr("elseExpr", expected->getElseExpr(), actual->getElseExpr());
}

bool CypherASTComparator::compareCaseBranch(std::string_view field, const CaseExpr::Branch& expected, const CaseExpr::Branch& actual) {
    const PathScope scope(_path, field);

    return compareEach("tests", expected._tests, actual._tests, &CypherASTComparator::compareCaseTest)
           && compareExpr("then", expected._then, actual._then);
}

bool CypherASTComparator::compareCaseTest(std::string_view field, const CaseExpr::Test& expected, const CaseExpr::Test& actual) {
    const PathScope scope(_path, field);

    return compareEnum("kind", expected._kind, actual._kind)
           && compareEnum("operator", expected._operator, actual._operator)
           && compareExpr("value", expected._value, actual._value);
}

bool CypherASTComparator::compareExistsExpr(const ExistsExpr* expected, const ExistsExpr* actual) {
    return compareQuery("body", expected->getBody(), actual->getBody())
           && comparePattern("predicatePattern", expected->getPredicatePattern(), actual->getPredicatePattern());
}

bool CypherASTComparator::compareCountSubqueryExpr(const CountSubqueryExpr* expected, const CountSubqueryExpr* actual) {
    return compareQuery("body", expected->getBody(), actual->getBody());
}

bool CypherASTComparator::compareCollectSubqueryExpr(const CollectSubqueryExpr* expected, const CollectSubqueryExpr* actual) {
    return compareQuery("body", expected->getBody(), actual->getBody());
}

bool CypherASTComparator::compareParameterExpr(const ParameterExpr* expected, const ParameterExpr* actual) {
    return compareParameter("parameter", expected->getParameter(), actual->getParameter());
}

bool CypherASTComparator::compareLiteral(std::string_view field, const Literal* expected, const Literal* actual) {
    const PathScope scope(_path, field);

    const Visit visit = visitNode(expected, actual, getClassName(expected), getClassName(actual));
    if (visit != Visit::PENDING) {
        return visit == Visit::EQUAL;
    }

    return compareLiteralContents(expected, actual);
}

bool CypherASTComparator::compareLiteralContents(const Literal* expected, const Literal* actual) {
    switch (expected->getKind()) {
        case Literal::Kind::NULL_LITERAL:
        case Literal::Kind::WILDCARD:
            return true;
        break;
        case Literal::Kind::BOOL: {
            const BoolLiteral* expectedBool = static_cast<const BoolLiteral*>(expected);
            const BoolLiteral* actualBool = static_cast<const BoolLiteral*>(actual);

            return compareBool("value", expectedBool->getValue(), actualBool->getValue());
        }
        break;
        case Literal::Kind::INTEGER: {
            const IntegerLiteral* expectedInteger = static_cast<const IntegerLiteral*>(expected);
            const IntegerLiteral* actualInteger = static_cast<const IntegerLiteral*>(actual);

            return compareInteger("value", expectedInteger->getValue(), actualInteger->getValue());
        }
        break;
        case Literal::Kind::DOUBLE: {
            const DoubleLiteral* expectedDouble = static_cast<const DoubleLiteral*>(expected);
            const DoubleLiteral* actualDouble = static_cast<const DoubleLiteral*>(actual);

            return compareDouble("value", expectedDouble->getValue(), actualDouble->getValue());
        }
        break;
        case Literal::Kind::STRING: {
            const StringLiteral* expectedString = static_cast<const StringLiteral*>(expected);
            const StringLiteral* actualString = static_cast<const StringLiteral*>(actual);

            return compareString("value", expectedString->getValue(), actualString->getValue());
        }
        break;
        case Literal::Kind::CHAR: {
            const CharLiteral* expectedChar = static_cast<const CharLiteral*>(expected);
            const CharLiteral* actualChar = static_cast<const CharLiteral*>(actual);

            return compareChar("value", expectedChar->getValue(), actualChar->getValue());
        }
        break;
        case Literal::Kind::LIST: {
            const ListLiteral* expectedList = static_cast<const ListLiteral*>(expected);
            const ListLiteral* actualList = static_cast<const ListLiteral*>(actual);

            return compareEach("items", expectedList->items(), actualList->items(), &CypherASTComparator::compareExpr);
        }
        break;
        case Literal::Kind::MAP: {
            const MapLiteral* expectedMap = static_cast<const MapLiteral*>(expected);
            const MapLiteral* actualMap = static_cast<const MapLiteral*>(actual);

            return compareEach("entries", *expectedMap, *actualMap, &CypherASTComparator::compareMapEntry);
        }
        break;
        case Literal::Kind::EMBEDDING: {
            const EmbeddingLiteral* expectedEmbedding = static_cast<const EmbeddingLiteral*>(expected);
            const EmbeddingLiteral* actualEmbedding = static_cast<const EmbeddingLiteral*>(actual);

            return compareEach("values", expectedEmbedding->getValue(), actualEmbedding->getValue(), &CypherASTComparator::compareFloat);
        }
        break;
        case Literal::Kind::NORMAL_FORM: {
            const NormalFormLiteral* expectedForm = static_cast<const NormalFormLiteral*>(expected);
            const NormalFormLiteral* actualForm = static_cast<const NormalFormLiteral*>(actual);

            return compareEnum("form", expectedForm->getForm(), actualForm->getForm());
        }
        break;
    }

    return true;
}

bool CypherASTComparator::compareMapEntry(std::string_view field,
                                          const std::pair<Symbol*, Expr*>& expected,
                                          const std::pair<Symbol*, Expr*>& actual) {
    const PathScope scope(_path, field);

    return compareSymbol("key", expected.first, actual.first)
           && compareExpr("value", expected.second, actual.second);
}
