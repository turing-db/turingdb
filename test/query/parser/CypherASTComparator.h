#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "ConditionalQuery.h"
#include "Projection.h"
#include "UnionQuery.h"
#include "expr/CaseExpr.h"
#include "expr/MapProjectionExpr.h"

namespace db {

class AllReduceExpr;
class BinaryExpr;
class CallStmt;
class CallSubqueryStmt;
class ChangeQuery;
class CollectSubqueryExpr;
class CountSubqueryExpr;
class CreateEdgePropertyIndexQuery;
class CreateGraphQuery;
class CreateNodePropertyIndexQuery;
class CreateStmt;
class CreateVectorIndexQuery;
class CypherAST;
class DeleteStmt;
class DeleteVectorIndexQuery;
class DropIndexQuery;
class EdgePattern;
class EmbeddingsSpec;
class EntityPattern;
class EntityTypeExpr;
class ExistsExpr;
class ExplainRequest;
class Expr;
class ExprChain;
class FunctionInvocationExpr;
class IndexExpr;
class InstallExtensionQuery;
class LabelExpr;
class Limit;
class ListComprehensionExpr;
class ListExpr;
class ListPredicateExpr;
class ListSliceExpr;
class Literal;
class LiteralExpr;
class LoadCSVStmt;
class LoadCommitQuery;
class LoadEmbeddingQuery;
class LoadGMLQuery;
class LoadGraphQuery;
class LoadJsonlQuery;
class LoadParquetQuery;
class LoadVectorQuery;
class LogicalExpr;
class MatchStmt;
class MergeStmt;
class NextSegmentStmt;
class NodePattern;
class OrderBy;
class OrderByItem;
class Parameter;
class ParameterExpr;
class Pattern;
class PatternComprehensionExpr;
class PatternElement;
class PropertyExpr;
class PropertyLookupExpr;
class QualifiedName;
class QuantifiedPath;
class QueryCommand;
class ReduceExpr;
class RemoveStmt;
class ReturnStmt;
class S3ConnectQuery;
class S3TransferQuery;
class SetItem;
class SetStmt;
class ShortestPathStmt;
class SinglePartQuery;
class Skip;
class Stmt;
class StmtContainer;
class StringExpr;
class Symbol;
class SymbolChain;
class SymbolExpr;
class TypedPropertySpec;
class UnaryExpr;
class UnwindStmt;
class VectorSearchStmt;
class WhereClause;
class WithStmt;
class YieldClause;

}

namespace turing::test {

class CypherASTComparator {
public:
    // Whether the two parse trees hold the same nodes and values, sharing a node wherever the
    // other does. Source locations and what only the analyzer sets are not compared. Otherwise
    // @param difference names the first difference, as the path from the query to its field.
    static bool compare(const db::CypherAST& expected, const db::CypherAST& actual, std::string& difference);

private:
    enum class Visit : uint8_t {
        DIFFERENT,
        EQUAL,
        PENDING,
    };

    struct Pairing {
        const void* _actual {nullptr};
        std::string _path;
    };

    std::string& _difference;
    std::string _path;
    std::unordered_map<const void*, Pairing> _expectedPairings;
    std::unordered_map<const void*, const void*> _actualPairings;

    explicit CypherASTComparator(std::string& difference);
    ~CypherASTComparator();

    Visit visitNode(const void* expected, const void* actual, std::string_view expectedClass, std::string_view actualClass);
    Visit visitNode(const void* expected, const void* actual, std::string_view className);
    Visit visitField(std::string_view field, const void* expected, const void* actual, std::string_view expectedClass, std::string_view actualClass);
    Visit visitField(std::string_view field, const void* expected, const void* actual, std::string_view className);
    Visit pairNodes(const void* expected, const void* actual);
    void appendClassName(std::string_view className);
    void reportDifference(std::string_view expected, std::string_view actual);

    template <typename Elements, typename CompareElement>
    bool compareEach(std::string_view field, const Elements& expected, const Elements& actual, CompareElement compareElement);

    template <typename Enum>
    bool compareEnum(std::string_view field, Enum expected, Enum actual);

    bool compareTypedPropertySpec(std::string_view field,
                                  const db::TypedPropertySpec& expected,
                                  const db::TypedPropertySpec& actual);

    bool compareSize(std::string_view field, size_t expected, size_t actual);
    bool comparePresence(std::string_view field, const void* expected, const void* actual, std::string_view className);
    bool compareBool(std::string_view field, bool expected, bool actual);
    bool compareInteger(std::string_view field, int64_t expected, int64_t actual);
    bool compareUnsigned(std::string_view field, uint64_t expected, uint64_t actual);
    bool compareBound(std::string_view field, int64_t expected, int64_t actual);
    bool compareDouble(std::string_view field, double expected, double actual);
    bool compareFloat(std::string_view field, float expected, float actual);
    bool compareChar(std::string_view field, char expected, char actual);
    bool compareString(std::string_view field, std::string_view expected, std::string_view actual);
    bool compareSymbol(std::string_view field, const db::Symbol* expected, const db::Symbol* actual);
    bool compareParameter(std::string_view field, const db::Parameter* expected, const db::Parameter* actual);
    bool compareQualifiedName(std::string_view field, const db::QualifiedName* expected, const db::QualifiedName* actual);
    bool compareSymbolChain(std::string_view field, const db::SymbolChain* expected, const db::SymbolChain* actual);
    bool compareEmbeddingSpec(std::string_view field, const db::EmbeddingsSpec& expected, const db::EmbeddingsSpec& actual);
    bool compareExplainRequest(std::string_view field, const db::ExplainRequest* expected, const db::ExplainRequest* actual);

    bool compareQuery(std::string_view field, const db::QueryCommand* expected, const db::QueryCommand* actual);
    bool compareSinglePartQuery(const db::SinglePartQuery* expected, const db::SinglePartQuery* actual);
    bool compareUnionQuery(const db::UnionQuery* expected, const db::UnionQuery* actual);
    bool compareUnionBranch(std::string_view field, const db::UnionQuery::Branch& expected, const db::UnionQuery::Branch& actual);
    bool compareConditionalQuery(const db::ConditionalQuery* expected, const db::ConditionalQuery* actual);
    bool compareConditionalBranch(std::string_view field, const db::ConditionalQuery::Branch& expected, const db::ConditionalQuery::Branch& actual);
    bool compareLoadGraphQuery(const db::LoadGraphQuery* expected, const db::LoadGraphQuery* actual);
    bool compareLoadJsonlQuery(const db::LoadJsonlQuery* expected, const db::LoadJsonlQuery* actual);
    bool compareChangeQuery(const db::ChangeQuery* expected, const db::ChangeQuery* actual);
    bool compareCreateGraphQuery(const db::CreateGraphQuery* expected, const db::CreateGraphQuery* actual);
    bool compareLoadGMLQuery(const db::LoadGMLQuery* expected, const db::LoadGMLQuery* actual);
    bool compareLoadParquetQuery(const db::LoadParquetQuery* expected, const db::LoadParquetQuery* actual);
    bool compareS3ConnectQuery(const db::S3ConnectQuery* expected, const db::S3ConnectQuery* actual);
    bool compareS3TransferQuery(const db::S3TransferQuery* expected, const db::S3TransferQuery* actual);
    bool compareCreateVectorIndexQuery(const db::CreateVectorIndexQuery* expected, const db::CreateVectorIndexQuery* actual);
    bool compareLoadVectorQuery(const db::LoadVectorQuery* expected, const db::LoadVectorQuery* actual);
    bool compareLoadEmbeddingQuery(const db::LoadEmbeddingQuery* expected, const db::LoadEmbeddingQuery* actual);
    bool compareDeleteVectorIndexQuery(const db::DeleteVectorIndexQuery* expected, const db::DeleteVectorIndexQuery* actual);
    bool compareLoadCommitQuery(const db::LoadCommitQuery* expected, const db::LoadCommitQuery* actual);
    bool compareInstallExtensionQuery(const db::InstallExtensionQuery* expected, const db::InstallExtensionQuery* actual);
    bool compareCreateNodePropertyIndexQuery(const db::CreateNodePropertyIndexQuery* expected, const db::CreateNodePropertyIndexQuery* actual);
    bool compareCreateEdgePropertyIndexQuery(const db::CreateEdgePropertyIndexQuery* expected, const db::CreateEdgePropertyIndexQuery* actual);
    bool compareDropIndexQuery(const db::DropIndexQuery* expected, const db::DropIndexQuery* actual);

    // A field declared with a subclass names the class already, so the Subclass variants
    // leave it out of the path
    bool compareStmt(std::string_view field, const db::Stmt* expected, const db::Stmt* actual);
    bool compareSubclassStmt(std::string_view field, const db::Stmt* expected, const db::Stmt* actual);
    bool compareStmtContents(const db::Stmt* expected, const db::Stmt* actual);
    bool compareStmtContainer(std::string_view field, const db::StmtContainer* expected, const db::StmtContainer* actual);
    bool compareMatchStmt(const db::MatchStmt* expected, const db::MatchStmt* actual);
    bool compareCallStmt(const db::CallStmt* expected, const db::CallStmt* actual);
    bool compareCallSubqueryStmt(const db::CallSubqueryStmt* expected, const db::CallSubqueryStmt* actual);
    bool compareCreateStmt(const db::CreateStmt* expected, const db::CreateStmt* actual);
    bool compareMergeStmt(const db::MergeStmt* expected, const db::MergeStmt* actual);
    bool compareSetStmt(const db::SetStmt* expected, const db::SetStmt* actual);
    bool compareSetItem(std::string_view field, const db::SetItem* expected, const db::SetItem* actual);
    bool compareRemoveStmt(const db::RemoveStmt* expected, const db::RemoveStmt* actual);
    bool compareDeleteStmt(const db::DeleteStmt* expected, const db::DeleteStmt* actual);
    bool compareReturnStmt(const db::ReturnStmt* expected, const db::ReturnStmt* actual);
    bool compareShortestPathStmt(const db::ShortestPathStmt* expected, const db::ShortestPathStmt* actual);
    bool compareLoadCSVStmt(const db::LoadCSVStmt* expected, const db::LoadCSVStmt* actual);
    bool compareVectorSearchStmt(const db::VectorSearchStmt* expected, const db::VectorSearchStmt* actual);
    bool compareUnwindStmt(const db::UnwindStmt* expected, const db::UnwindStmt* actual);
    bool compareWithStmt(const db::WithStmt* expected, const db::WithStmt* actual);
    bool compareNextSegmentStmt(const db::NextSegmentStmt* expected, const db::NextSegmentStmt* actual);
    bool compareProjection(std::string_view field, const db::Projection* expected, const db::Projection* actual);
    bool compareProjectionItem(std::string_view field, const db::Projection::ReturnItem& expected, const db::Projection::ReturnItem& actual);
    bool compareOrderBy(std::string_view field, const db::OrderBy* expected, const db::OrderBy* actual);
    bool compareOrderByItem(std::string_view field, const db::OrderByItem* expected, const db::OrderByItem* actual);
    bool compareSkip(std::string_view field, const db::Skip* expected, const db::Skip* actual);
    bool compareLimit(std::string_view field, const db::Limit* expected, const db::Limit* actual);
    bool compareWhereClause(std::string_view field, const db::WhereClause* expected, const db::WhereClause* actual);
    bool compareYieldClause(std::string_view field, const db::YieldClause* expected, const db::YieldClause* actual);

    bool comparePattern(std::string_view field, const db::Pattern* expected, const db::Pattern* actual);
    bool comparePatternElement(std::string_view field, const db::PatternElement* expected, const db::PatternElement* actual);
    bool compareEntity(std::string_view field, const db::EntityPattern* expected, const db::EntityPattern* actual);
    bool compareSubclassEntity(std::string_view field, const db::EntityPattern* expected, const db::EntityPattern* actual);
    bool compareEntityContents(const db::EntityPattern* expected, const db::EntityPattern* actual);
    bool compareNodePattern(const db::NodePattern* expected, const db::NodePattern* actual);
    bool compareEdgePattern(const db::EdgePattern* expected, const db::EdgePattern* actual);
    bool compareHops(const db::EdgePattern* expected, const db::EdgePattern* actual);
    bool compareQuantifiedPath(std::string_view field, const db::QuantifiedPath* expected, const db::QuantifiedPath* actual);
    bool compareLabelExpr(std::string_view field, const db::LabelExpr* expected, const db::LabelExpr* actual);

    bool compareExpr(std::string_view field, const db::Expr* expected, const db::Expr* actual);
    bool compareSubclassExpr(std::string_view field, const db::Expr* expected, const db::Expr* actual);
    bool compareExprContents(const db::Expr* expected, const db::Expr* actual);
    bool compareExprChain(std::string_view field, const db::ExprChain* expected, const db::ExprChain* actual);
    bool compareBinaryExpr(const db::BinaryExpr* expected, const db::BinaryExpr* actual);
    bool compareLogicalExpr(const db::LogicalExpr* expected, const db::LogicalExpr* actual);
    bool compareUnaryExpr(const db::UnaryExpr* expected, const db::UnaryExpr* actual);
    bool compareStringExpr(const db::StringExpr* expected, const db::StringExpr* actual);
    bool compareEntityTypeExpr(const db::EntityTypeExpr* expected, const db::EntityTypeExpr* actual);
    bool comparePropertyExpr(const db::PropertyExpr* expected, const db::PropertyExpr* actual);
    bool comparePropertyLookupExpr(const db::PropertyLookupExpr* expected, const db::PropertyLookupExpr* actual);
    bool compareSymbolExpr(const db::SymbolExpr* expected, const db::SymbolExpr* actual);
    bool compareLiteralExpr(const db::LiteralExpr* expected, const db::LiteralExpr* actual);
    bool compareFunctionInvocationExpr(const db::FunctionInvocationExpr* expected, const db::FunctionInvocationExpr* actual);
    bool compareIndexExpr(const db::IndexExpr* expected, const db::IndexExpr* actual);
    bool compareListExpr(const db::ListExpr* expected, const db::ListExpr* actual);
    bool compareListSliceExpr(const db::ListSliceExpr* expected, const db::ListSliceExpr* actual);
    bool compareListComprehensionExpr(const db::ListComprehensionExpr* expected, const db::ListComprehensionExpr* actual);
    bool comparePatternComprehensionExpr(const db::PatternComprehensionExpr* expected, const db::PatternComprehensionExpr* actual);
    bool compareListPredicateExpr(const db::ListPredicateExpr* expected, const db::ListPredicateExpr* actual);
    bool compareReduceExpr(const db::ReduceExpr* expected, const db::ReduceExpr* actual);
    bool compareAllReduceExpr(const db::AllReduceExpr* expected, const db::AllReduceExpr* actual);
    bool compareMapProjectionExpr(const db::MapProjectionExpr* expected, const db::MapProjectionExpr* actual);
    bool compareMapProjectionItem(std::string_view field, const db::MapProjectionExpr::Item& expected, const db::MapProjectionExpr::Item& actual);
    bool compareCaseExpr(const db::CaseExpr* expected, const db::CaseExpr* actual);
    bool compareCaseBranch(std::string_view field, const db::CaseExpr::Branch& expected, const db::CaseExpr::Branch& actual);
    bool compareCaseTest(std::string_view field, const db::CaseExpr::Test& expected, const db::CaseExpr::Test& actual);
    bool compareExistsExpr(const db::ExistsExpr* expected, const db::ExistsExpr* actual);
    bool compareCountSubqueryExpr(const db::CountSubqueryExpr* expected, const db::CountSubqueryExpr* actual);
    bool compareCollectSubqueryExpr(const db::CollectSubqueryExpr* expected, const db::CollectSubqueryExpr* actual);
    bool compareParameterExpr(const db::ParameterExpr* expected, const db::ParameterExpr* actual);

    bool compareLiteral(std::string_view field, const db::Literal* expected, const db::Literal* actual);
    bool compareLiteralContents(const db::Literal* expected, const db::Literal* actual);
    bool compareMapEntry(std::string_view field, const std::pair<db::Symbol*, db::Expr*>& expected, const std::pair<db::Symbol*, db::Expr*>& actual);
};

}
