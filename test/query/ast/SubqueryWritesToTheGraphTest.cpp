#include <gtest/gtest.h>

#include "CypherAST.h"
#include "SinglePartQuery.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"

using namespace db;

// Two questions a CALL subquery is asked, which its body answers differently: whether
// anything under it writes, and whether the clause itself belongs with the updating ones.
// A body that writes and ends on RETURN answers yes to the first and no to the second
class SubqueryWritesToTheGraphTest : public ::testing::Test {
protected:
    SubqueryWritesToTheGraphTest()
        : _ast(nullptr, "")
    {
    }

    SinglePartQuery* query() {
        SinglePartQuery* query = SinglePartQuery::create(&_ast);
        query->setStmts(StmtContainer::create(&_ast));

        return query;
    }

    Stmt* create() { return CreateStmt::create(&_ast, nullptr); }
    Stmt* match() { return MatchStmt::create(&_ast, nullptr); }
    ReturnStmt* returns() { return ReturnStmt::create(&_ast, nullptr); }

    CallSubqueryStmt* subqueryOver(SinglePartQuery* body) {
        return CallSubqueryStmt::create(&_ast, body);
    }

    CypherAST _ast;
};

TEST_F(SubqueryWritesToTheGraphTest, countsAReturningBodyThatWrites) {
    SinglePartQuery* body = query();
    body->addStmt(create());
    body->setReturnStmt(returns());

    SinglePartQuery* outer = query();
    CallSubqueryStmt* subquery = subqueryOver(body);
    outer->addStmt(subquery);

    ASSERT_TRUE(subquery->isReturning());

    EXPECT_TRUE(outer->writesToTheGraph());

    // The clause still belongs with the reading ones: its rows join the ones in flight
    EXPECT_FALSE(Stmt::isUpdating(subquery));
}

TEST_F(SubqueryWritesToTheGraphTest, countsAReturningBodyThatWritesThroughANestedSubquery) {
    SinglePartQuery* inner = query();
    inner->addStmt(create());
    inner->setReturnStmt(returns());

    SinglePartQuery* body = query();
    body->addStmt(subqueryOver(inner));
    body->setReturnStmt(returns());

    SinglePartQuery* outer = query();
    outer->addStmt(subqueryOver(body));

    EXPECT_TRUE(outer->writesToTheGraph());
}

TEST_F(SubqueryWritesToTheGraphTest, countsAUnitBodyThatWrites) {
    SinglePartQuery* body = query();
    body->addStmt(create());

    SinglePartQuery* outer = query();
    CallSubqueryStmt* subquery = subqueryOver(body);
    outer->addStmt(subquery);

    EXPECT_TRUE(outer->writesToTheGraph());
    EXPECT_TRUE(Stmt::isUpdating(subquery));
}

TEST_F(SubqueryWritesToTheGraphTest, countsNoBodyThatOnlyReads) {
    SinglePartQuery* body = query();
    body->addStmt(match());
    body->setReturnStmt(returns());

    SinglePartQuery* outer = query();
    CallSubqueryStmt* subquery = subqueryOver(body);
    outer->addStmt(subquery);

    EXPECT_FALSE(outer->writesToTheGraph());
    EXPECT_FALSE(Stmt::isUpdating(subquery));
}
