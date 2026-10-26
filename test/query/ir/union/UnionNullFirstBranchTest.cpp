#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A first branch answering null leaves the column the type the other branches give it, so
// what reads the column after the union tests each row for null. In simpledb KNOWS_WELL
// leaves Remy for Adam and Adam for Remy.
class UnionNullFirstBranchTest : public WriteQueryTest {
};

TEST_F(UnionNullFirstBranchTest, testsEachRowOfAScalarColumnForNull) {
    expectRows("CALL { RETURN null AS x UNION ALL RETURN 1 AS x } RETURN x IS NULL",
               {{"true"}, {"false"}});
    expectRows("CALL { RETURN null AS x UNION ALL RETURN 1 AS x } WITH x WHERE x IS NOT NULL RETURN x",
               {{"1"}});
}

TEST_F(UnionNullFirstBranchTest, testsEachRowOfANodeColumnForNull) {
    expectRows("CALL { RETURN null AS x UNION ALL MATCH (n:Person {name: 'Remy'}) RETURN n AS x } "
               "RETURN x IS NULL",
               {{"true"}, {"false"}});
}

TEST_F(UnionNullFirstBranchTest, testsEachRowOfAPathColumnForNull) {
    expectRows("CALL { RETURN null AS x UNION ALL MATCH p = (:Person)-[:KNOWS_WELL]->() RETURN p AS x } "
               "RETURN x IS NULL, count(*)",
               {{"false", "2"}, {"true", "1"}});
}

TEST_F(UnionNullFirstBranchTest, testsEachRowOfAWhenColumnForNull) {
    expectRows("MATCH (n:Person) "
               "CALL (n) { WHEN n.isFrench THEN RETURN null AS x ELSE RETURN n.name AS x } "
               "RETURN x IS NULL, count(*)",
               {{"true", "4"}, {"false", "4"}});
}
