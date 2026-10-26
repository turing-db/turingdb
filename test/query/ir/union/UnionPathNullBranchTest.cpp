#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A column of paths holds a branch answering null, as the empty path an OPTIONAL MATCH
// leaves where it finds none. In simpledb KNOWS_WELL leaves Remy for Adam and Adam for Remy,
// and there are 8 people.
class UnionPathNullBranchTest : public WriteQueryTest {
};

TEST_F(UnionPathNullBranchTest, holdsANullBesideAPath) {
    expectRows("CALL { MATCH p = (:Person)-[:KNOWS_WELL]->() RETURN p AS x UNION ALL RETURN null AS x } "
               "RETURN x IS NULL, count(*)",
               {{"false", "2"}, {"true", "1"}});
}

TEST_F(UnionPathNullBranchTest, returnsTheNullPath) {
    expectRows("CALL { MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL]->() RETURN p AS q UNION ALL RETURN null AS q } "
               "RETURN length(q)",
               {{"1"}, {"null"}});
}

TEST_F(UnionPathNullBranchTest, holdsANullBesideAPathInAWhen) {
    expectRows("MATCH (a:Person) "
               "CALL (a) { "
               "  WHEN a.name = 'Remy' THEN MATCH p = (a)-[:KNOWS_WELL]->() RETURN p AS x "
               "  ELSE RETURN null AS x "
               "} "
               "RETURN a.name, x IS NULL",
               {{"Remy", "false"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "true"},
                {"Martina", "true"},
                {"Suhas", "true"},
                {"Cyrus", "true"},
                {"Doruk", "true"}});
}
