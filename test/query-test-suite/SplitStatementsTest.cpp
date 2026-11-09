#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "QueryTestRunner.h"
#include "TuringException.h"

using namespace turing::test;

namespace {

void expectStatements(std::string_view script, const std::vector<std::string>& expected) {
    std::vector<std::string> statements;
    QueryTestRunner::splitStatements(statements, script);

    EXPECT_EQ(statements, expected) << script;
}

}

TEST(SplitStatementsTest, semicolonsSeparateStatements) {
    expectStatements("CREATE (:A); CREATE (:B);", {"CREATE (:A)", "CREATE (:B)"});
}

TEST(SplitStatementsTest, lastStatementNeedsNoSemicolon) {
    expectStatements("CREATE (:A);\nMATCH (a:A)\nRETURN a", {"CREATE (:A)", "MATCH (a:A)\nRETURN a"});
}

TEST(SplitStatementsTest, emptyStatementsAreDropped) {
    expectStatements(" ;; ;\n", {});
    expectStatements("", {});
}

TEST(SplitStatementsTest, semicolonInStringDoesNotSeparate) {
    expectStatements("CREATE (:A {s: 'x;y'}); CREATE (:B {s: \"u\\\";v\"})",
                     {"CREATE (:A {s: 'x;y'})", "CREATE (:B {s: \"u\\\";v\"})"});
}

TEST(SplitStatementsTest, semicolonInEscapedNameDoesNotSeparate) {
    expectStatements("CREATE (:`A;B`); CREATE (:C)", {"CREATE (:`A;B`)", "CREATE (:C)"});
}

TEST(SplitStatementsTest, commentsAroundStatementsAreDropped) {
    expectStatements("// first; the nodes\nCREATE (:A) /* ; */ ;\n/* then; */ CREATE (:B) // done;",
                     {"CREATE (:A)", "CREATE (:B)"});
}

TEST(SplitStatementsTest, commentInsideStatementIsKept) {
    expectStatements("CREATE (:A) // one; two\nRETURN 1", {"CREATE (:A) // one; two\nRETURN 1"});
}

TEST(SplitStatementsTest, unclosedCommentThrows) {
    std::vector<std::string> statements;
    EXPECT_THROW(QueryTestRunner::splitStatements(statements, "CREATE (:A); /* ;"), TuringException);
}
