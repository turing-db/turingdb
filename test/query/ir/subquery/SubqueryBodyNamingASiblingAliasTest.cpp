#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A body declaring a variable under the alias of an earlier item of the same projection: the
// alias names no row there, so the variable is the body's own. The graph holds 8 people and
// 18 nodes.
class SubqueryBodyNamingASiblingAliasTest : public WriteQueryTest {
};

TEST_F(SubqueryBodyNamingASiblingAliasTest, countsTheNodesItMatchesUnderTheAlias) {
    expectRows("MATCH (n:Person) RETURN n.name AS x, COUNT { MATCH (x:Person) } AS c",
               {{"Remy", "8"}, {"Adam", "8"}, {"Maxime", "8"}, {"Luc", "8"},
                {"Martina", "8"}, {"Suhas", "8"}, {"Cyrus", "8"}, {"Doruk", "8"}});

    expectRows("MATCH (n:Person) RETURN n.name AS x, COUNT { MATCH (x) } AS c",
               {{"Remy", "18"}, {"Adam", "18"}, {"Maxime", "18"}, {"Luc", "18"},
                {"Martina", "18"}, {"Suhas", "18"}, {"Cyrus", "18"}, {"Doruk", "18"}});
}

TEST_F(SubqueryBodyNamingASiblingAliasTest, countsTheElementsItUnwindsUnderTheAlias) {
    expectRows("MATCH (n:Person) RETURN n.name AS x, COUNT { UNWIND [1, 2] AS x RETURN x } AS c",
               {{"Remy", "2"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "2"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "2"}});
}

TEST_F(SubqueryBodyNamingASiblingAliasTest, existsForTheNodesItMatchesUnderTheAlias) {
    expectRows("MATCH (n:Person) RETURN n.name AS x, EXISTS { MATCH (x:Person) } AS e",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "true"}, {"Luc", "true"},
                {"Martina", "true"}, {"Suhas", "true"}, {"Cyrus", "true"}, {"Doruk", "true"}});
}
