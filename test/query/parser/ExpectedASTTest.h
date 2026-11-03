#pragma once

#include <stdint.h>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "ConditionalQuery.h"
#include "EdgePattern.h"
#include "Literal.h"
#include "NodePattern.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Projection.h"
#include "QuantifiedPath.h"
#include "SinglePartQuery.h"
#include "UnicodeNormalForm.h"
#include "UnionQuery.h"
#include "WhereClause.h"
#include "YieldClause.h"
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
#include "stmt/UnwindStmt.h"
#include "stmt/VectorSearchStmt.h"
#include "stmt/WithStmt.h"

namespace db {

class CypherAST;

}

namespace turing::test {

// A query is checked against the AST it must parse to, built here with the AST's own create
// functions, or against the report it must fail with. Every query one edit away from it must
// parse or report an error too.
class ExpectedASTTest : public ::testing::Test {
protected:
    struct NodeFields {
        std::string_view _symbol;
        db::LabelExpr* _labels {nullptr};
        db::MapLiteral* _properties {nullptr};
        db::WhereClause* _where {nullptr};
    };

    struct EdgeFields {
        db::EdgePattern::Direction _direction {db::EdgePattern::Direction::Undirected};
        std::string_view _symbol;
        db::LabelExpr* _labels {nullptr};
        db::MapLiteral* _properties {nullptr};
        db::WhereClause* _where {nullptr};
        db::QuantifiedPath* _range {nullptr};
    };

    struct WalkFields {
        db::EdgePattern::Direction _direction {db::EdgePattern::Direction::Undirected};
        db::QuantifiedPath* _quantifier {nullptr};
        std::vector<db::EdgePattern*> _hops;
        std::vector<db::NodePattern*> _hopNodes;
        db::WhereClause* _hopWhere {nullptr};
    };

    struct MatchFields {
        db::Pattern* _pattern {nullptr};
        bool _optional {false};
        db::OrderBy* _orderBy {nullptr};
        db::Skip* _skip {nullptr};
        db::Limit* _limit {nullptr};
    };

    struct ProjectionFields {
        std::vector<db::Expr*> _items;
        bool _distinct {false};
        bool _all {false};
        db::OrderBy* _orderBy {nullptr};
        db::Skip* _skip {nullptr};
        db::Limit* _limit {nullptr};
    };

    struct WithFields {
        db::Projection* _projection {nullptr};
        db::WhereClause* _where {nullptr};
        bool _segmentReturn {false};
    };

    struct CallFields {
        db::FunctionInvocationExpr* _invocation {nullptr};
        db::YieldClause* _yield {nullptr};
        bool _optional {false};
        bool _standalone {false};
    };

    struct SubqueryFields {
        db::RegularQuery* _body {nullptr};
        std::vector<std::string_view> _imports;
        bool _scope {false};
        bool _importsEverything {false};
        bool _optional {false};
    };

    struct LoadCSVFields {
        std::string_view _path;
        std::string_view _alias;
        bool _headers {false};
        bool _skipOnError {false};
    };

    struct VectorSearchFields {
        std::string_view _index;
        uint64_t _neighbours {0};
        db::EmbeddingLiteral* _queryVector {nullptr};
        db::YieldClause* _yield {nullptr};
    };

    ExpectedASTTest();
    ~ExpectedASTTest() override;

    void expectParse(std::string_view query, std::initializer_list<const db::QueryCommand*> queries);
    void expectError(std::string_view query, std::string_view report);

    db::CypherAST* getExpectedAST() const { return _expected.get(); }

    db::Symbol* name(std::string_view spelling);

    db::LiteralExpr* literal(db::Literal* value);
    db::LiteralExpr* integer(int64_t value);
    db::LiteralExpr* real(double value);
    db::LiteralExpr* string(std::string_view value);
    db::LiteralExpr* boolean(bool value);
    db::LiteralExpr* null();
    db::LiteralExpr* wildcard();
    db::LiteralExpr* normalForm(db::UnicodeNormalForm form);
    db::LiteralExpr* list(std::initializer_list<db::Expr*> items);
    db::LiteralExpr* map(std::initializer_list<std::pair<std::string_view, db::Expr*>> entries);
    db::MapLiteral* properties(std::initializer_list<std::pair<std::string_view, db::Expr*>> entries);
    db::EmbeddingLiteral* embedding(std::initializer_list<float> values);

    db::SymbolExpr* symbol(std::string_view spelling);
    db::PropertyExpr* property(std::initializer_list<std::string_view> names);
    db::PropertyLookupExpr* lookup(db::Expr* base, std::string_view propertyName);
    db::ParameterExpr* parameter(std::string_view spelling);
    db::BinaryExpr* binary(db::BinaryOperator binaryOperator, db::Expr* lhs, db::Expr* rhs);
    db::UnaryExpr* unary(db::UnaryOperator unaryOperator, db::Expr* operand);
    db::StringExpr* stringExpr(db::StringOperator stringOperator, db::Expr* lhs, db::Expr* rhs);
    db::LogicalExpr* logical(db::LogicalOperator logicalOperator, std::initializer_list<db::Expr*> operands);
    db::LogicalExpr* comparisonChain(db::Expr* first, std::initializer_list<std::pair<db::BinaryOperator, db::Expr*>> links);
    db::IndexExpr* index(db::Expr* base, db::Expr* position);
    db::ListSliceExpr* slice(db::Expr* base, db::Expr* from, db::Expr* to);
    db::ListComprehensionExpr* listComprehension(std::string_view variable,
                                                 db::Expr* source,
                                                 db::Expr* predicate = nullptr,
                                                 db::Expr* projection = nullptr);
    db::ListPredicateExpr* listPredicate(db::ListPredicateExpr::Quantifier quantifier,
                                         db::ListComprehensionExpr* comprehension);
    db::PatternComprehensionExpr* patternComprehension(db::MatchStmt* match, db::Expr* projection);
    db::ReduceExpr* reduce(std::string_view accumulator,
                           db::Expr* initialValue,
                           std::string_view item,
                           db::Expr* source,
                           db::Expr* expression);
    db::AllReduceExpr* allReduce(std::string_view accumulator,
                                 db::Expr* initialValue,
                                 std::string_view item,
                                 db::Expr* source,
                                 db::Expr* expression,
                                 db::Expr* predicate);
    db::CaseExpr* caseExpr(db::Expr* subject, std::initializer_list<db::CaseExpr::Branch> branches, db::Expr* elseExpr = nullptr);
    db::FunctionInvocationExpr* invocation(std::initializer_list<std::string_view> names,
                                           std::initializer_list<db::Expr*> arguments);
    db::FunctionInvocationExpr* distinct(db::FunctionInvocationExpr* invocation);
    db::ExistsExpr* exists(db::RegularQuery* body);
    db::ExistsExpr* patternPredicate(db::Pattern* predicate);
    db::CountSubqueryExpr* countSubquery(db::RegularQuery* body);
    db::CollectSubqueryExpr* collectSubquery(db::RegularQuery* body);
    db::MapProjectionExpr* mapProjection(std::string_view base, std::initializer_list<db::MapProjectionExpr::Item> items);
    db::EntityTypeExpr* entityType(std::string_view variable, db::LabelExpr* labels);
    db::EntityTypeExpr* labelTest(db::Expr* operand, db::LabelExpr* labels);

    template <typename ExprType>
    ExprType* parenthesized(ExprType* expr) {
        expr->setParenthesized();
        return expr;
    }

    template <typename ExprType>
    ExprType* named(ExprType* expr, std::string_view alias) {
        expr->setName(alias);
        return expr;
    }

    db::NodePattern* node(const NodeFields& fields);
    db::EdgePattern* edge(const EdgeFields& fields);
    db::EdgePattern* walk(const WalkFields& fields);
    db::QuantifiedPath* quantifier(int64_t lower, int64_t upper);
    db::LabelExpr* labels(std::initializer_list<std::string_view> names);
    db::LabelExpr* labelParameter(std::string_view spelling);
    db::LabelExpr* labelNot(db::LabelExpr* operand);
    db::LabelExpr* labelAnd(std::initializer_list<db::LabelExpr*> operands);
    db::LabelExpr* labelOr(std::initializer_list<db::LabelExpr*> operands);
    db::PatternElement* path(std::initializer_list<db::EntityPattern*> entities);
    db::PatternElement* namedPath(std::string_view pathName, std::initializer_list<db::EntityPattern*> entities);
    db::Pattern* pattern(std::initializer_list<db::PatternElement*> elements, db::WhereClause* filter = nullptr);
    db::WhereClause* where(db::Expr* predicate);

    db::MatchStmt* match(const MatchFields& fields);
    db::Projection* projection(const ProjectionFields& fields);
    db::ReturnStmt* returning(std::initializer_list<db::Expr*> items);
    db::ReturnStmt* returning(db::Projection* columns);
    db::WithStmt* with(const WithFields& fields);
    db::UnwindStmt* unwind(db::Expr* list, std::string_view variable);
    db::CallStmt* call(const CallFields& fields);
    db::CallSubqueryStmt* callSubquery(const SubqueryFields& fields);
    db::NextSegmentStmt* nextSegment(db::CallSubqueryStmt* body);
    db::CreateStmt* createStmt(db::Pattern* created);
    db::MergeStmt* merge(db::Pattern* merged, db::SetStmt* onCreate = nullptr, db::SetStmt* onMatch = nullptr);
    db::SetStmt* set(std::initializer_list<db::SetItem*> items);
    db::SetItem* setProperty(db::PropertyExpr* target, db::Expr* value);
    db::SetItem* setLabels(db::EntityTypeExpr* labelsSet);
    db::SetItem* addProperties(std::string_view variable, db::Expr* value);
    db::SetItem* replaceProperties(std::string_view variable, db::Expr* value);
    db::RemoveStmt* remove(std::initializer_list<db::PropertyExpr*> removed);
    db::DeleteStmt* deleteStmt(std::initializer_list<db::Expr*> deleted);
    db::DeleteStmt* detachDelete(std::initializer_list<db::Expr*> deleted);
    db::OrderBy* orderBy(std::initializer_list<std::pair<db::Expr*, db::OrderByType>> items);
    db::Skip* skip(db::Expr* count);
    db::Limit* limit(db::Expr* count);
    db::YieldClause* yield(std::initializer_list<db::SymbolExpr*> items, db::WhereClause* filter = nullptr);
    db::YieldClause* yieldAll();
    db::SymbolExpr* yieldItem(std::string_view yielded, std::string_view alias);
    db::LoadCSVStmt* loadCSV(const LoadCSVFields& fields);
    db::VectorSearchStmt* vectorSearch(const VectorSearchFields& fields);
    db::ShortestPathStmt* shortestPath(std::string_view source,
                                       std::string_view target,
                                       std::string_view edgeProperty,
                                       std::string_view distance,
                                       std::string_view shortest);

    db::SinglePartQuery* query(std::initializer_list<db::Stmt*> stmts, db::ReturnStmt* returnStmt = nullptr);
    db::UnionQuery* unionQuery(db::RegularQuery* first, std::initializer_list<db::UnionQuery::Branch> rest);
    db::ConditionalQuery* conditional(std::initializer_list<db::ConditionalQuery::Branch> branches);

private:
    std::unique_ptr<db::CypherAST> _expected;

    void expectMutantsWellFormed(std::string_view query);
    void resetExpectedAST();
};

}
