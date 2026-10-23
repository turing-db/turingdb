#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

using Rows = std::vector<StringRowSink::Row>;

// Relationship isomorphism holds inside every pattern scope - an OPTIONAL MATCH, an EXISTS or
// COUNT subquery, a CALL body, a UNION branch - and stops at its boundary. Expected counts are
// an enumeration of simpledb's 18 edges under openCypher's rule.
class EdgeUniquenessScopeTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(EdgeUniquenessScopeTest, padsTheRowsAnOptionalMatchLosesToTheRule) {
    expectRows("MATCH (a) OPTIONAL MATCH (a)-[e1]->(b)<-[e2]-(a) RETURN count(*), count(b)", {{"18", "0"}});
}

TEST_F(EdgeUniquenessScopeTest, appliesInsideAnOptionalMatch) {
    expectRows("MATCH (a) OPTIONAL MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*), count(c)", {{"35", "26"}});
}

TEST_F(EdgeUniquenessScopeTest, keepsTheTwoCyclesAnOptionalMatchCloses) {
    expectRows("MATCH (a) OPTIONAL MATCH (a)-[e1]-(b)-[e2]-(a) WITH a, count(b) AS n WHERE n > 0 RETURN a.name, n ORDER BY a.name",
               {{"Adam", "2"}, {"Ghosts", "2"}, {"Remy", "4"}});
}

TEST_F(EdgeUniquenessScopeTest, appliesInsideAnExistsSubquery) {
    expectRows("MATCH (a) WHERE EXISTS { (a)-[e1]->(b)<-[e2]-(a) } RETURN count(*)", {{"0"}});
    expectRows("MATCH (a) WHERE NOT EXISTS { (a)-[e1]->(b)<-[e2]-(a) } RETURN count(*)", {{"18"}});
    expectRows("MATCH (a) WHERE EXISTS { (a)-[e1]-(b)-[e2]-(a) } RETURN count(*)", {{"3"}});
}

TEST_F(EdgeUniquenessScopeTest, appliesInsideACountSubquery) {
    expectRows("MATCH (a) WITH a, COUNT { (a)-[e1]->(b)-[e2]-(c) } AS n WHERE n > 0 RETURN a.name, n ORDER BY a.name",
               {{"Adam", "7"},
                {"Cyrus", "2"},
                {"Doruk", "2"},
                {"Ghosts", "5"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Maxime", "1"},
                {"Remy", "5"},
                {"Suhas", "2"}});
}

TEST_F(EdgeUniquenessScopeTest, appliesInsideACallBody) {
    expectRows("CALL { MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*) AS n } RETURN n", {{"26"}});
}

TEST_F(EdgeUniquenessScopeTest, doesNotReachACallBody) {
    expectRows("MATCH (a)-[e1]->(b) CALL (b) { MATCH (b)-[e2]-(c) RETURN count(*) AS n } RETURN sum(n)", {{"44"}});
    expectRows("MATCH (a)-[e1]->(b) CALL { WITH b MATCH (b)-[e2]-(c) RETURN count(*) AS n } RETURN sum(n)", {{"44"}});
}

TEST_F(EdgeUniquenessScopeTest, scopesEachUnionBranch) {
    expectRows("MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*) AS n "
               "UNION ALL "
               "MATCH (a)-[e1]->(b) MATCH (b)-[e2]-(c) RETURN count(*) AS n",
               {{"26"}, {"44"}});
}

TEST_F(EdgeUniquenessScopeTest, excludesAnEdgeAnEarlierClauseBoundOnceItsPatternRebindsIt) {
    expectRows("MATCH ()-[e1]->() WITH e1 MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*)", {{"26"}});
    expectRows("MATCH (a)-[e1]->(b) WITH a, b, e1 MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*)", {{"26"}});
}
