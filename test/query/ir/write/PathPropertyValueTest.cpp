#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"
#include "versioning/ChangeID.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A path is a value of the query that matched it: a property cannot hold one, inside a list
// or a map at any depth, as Neo4j turns it away. Remy KNOWS_WELL Adam, so each query below
// has one path to write.
class PathPropertyValueTest : public WriteQueryTest {
protected:
    static constexpr std::string_view REJECTION = "A property value cannot hold a path";

    void expectWriteRejected(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query: " << query << " was accepted";
        EXPECT_NE(status.getError().find(REJECTION), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();

        const QueryStatus commit = runWrite("COMMIT", changeID);
        ASSERT_TRUE(commit.isOk()) << commit.getError();

        submit(changeID);
    }
};

TEST_F(PathPropertyValueTest, rejectsAListHoldingAPath) {
    expectWriteRejected("MATCH p=(n:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) SET n.paths = [p]");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.paths", {{"null"}});
}

TEST_F(PathPropertyValueTest, rejectsAMapHoldingAPath) {
    expectWriteRejected("MATCH p=(n:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) SET n.route = {path: p}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.route", {{"null"}});
}

TEST_F(PathPropertyValueTest, rejectsAPathInAMapInAList) {
    expectWriteRejected("MATCH p=(n:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) SET n.routes = [[1, {path: p}]]");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.routes", {{"null"}});
}

TEST_F(PathPropertyValueTest, createsNoNodeWhenItsPropertyHoldsAPath) {
    expectWriteRejected("MATCH p=(:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) CREATE (:Route {paths: [p]})");
    expectRows("MATCH (r:Route) RETURN count(r)", {{"0"}});
}

TEST_F(PathPropertyValueTest, createsNoEdgeWhenItsPropertyHoldsAPath) {
    expectWriteRejected("MATCH p=(r:Person {name: 'Remy'})-[:KNOWS_WELL]->(a:Person) CREATE (r)-[:ROUTED {paths: [p]}]->(a)");
    expectRows("MATCH ()-[e:ROUTED]->() RETURN count(e)", {{"0"}});
}

TEST_F(PathPropertyValueTest, writesTheLengthsOfThePaths) {
    applyWrite("MATCH p=(n:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) SET n.hops = [length(p)]");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.hops", {{"[1]"}});
}
