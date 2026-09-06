#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A node or edge pattern that binds no variable still carries an inline property map, and
// the map filters exactly as it does on a named pattern. The generated name the anonymous
// pattern declares stays out of reach of the query, which is what AnonymousMatchTest pins.
class AnonymousPatternPropertyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    void expectError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(AnonymousPatternPropertyTest, filtersOnAnAnonymousStartNode) {
    expectRows("MATCH (:Person {name: 'Remy'})-[:KNOWS_WELL]->(other:Person) RETURN other.name",
               {{"Adam"}});
}

TEST_F(AnonymousPatternPropertyTest, filtersOnAnAnonymousEndNode) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(:Interest {name: 'Computers'}) RETURN p.name",
               {{"Remy"}, {"Luc"}});
}

TEST_F(AnonymousPatternPropertyTest, filtersOnAnAnonymousNodeWithoutALabel) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->({name: 'Cooking'}) RETURN p.name",
               {{"Adam"}, {"Martina"}});
}

TEST_F(AnonymousPatternPropertyTest, filtersOnAnAnonymousEdge) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN {proficiency: 'expert'}]->(i:Interest) RETURN p.name, i.name",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Maxime", "Padel"}});
}

TEST_F(AnonymousPatternPropertyTest, filtersOnTwoPropertiesOfOneAnonymousNode) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->({name: 'Ghosts', isReal: true}) RETURN p.name",
               {{"Remy"}});
}

TEST_F(AnonymousPatternPropertyTest, matchesTheSameRowsAsTheNamedPattern) {
    StringRowSink named;
    StringRowSink anonymous;
    QueryStatus namedStatus;
    QueryStatus anonymousStatus;

    runQuery("MATCH (p:Person)-[e:INTERESTED_IN {proficiency: 'expert'}]->(i:Interest {isReal: true}) RETURN p.name",
             named, namedStatus);
    runQuery("MATCH (p:Person)-[:INTERESTED_IN {proficiency: 'expert'}]->(:Interest {isReal: true}) RETURN p.name",
             anonymous, anonymousStatus);

    ASSERT_TRUE(namedStatus.isOk()) << namedStatus.getError();
    ASSERT_TRUE(anonymousStatus.isOk()) << anonymousStatus.getError();

    std::vector<StringRowSink::Row> namedRows;
    std::vector<StringRowSink::Row> anonymousRows;
    named.sortedRows(namedRows);
    anonymous.sortedRows(anonymousRows);

    EXPECT_EQ(anonymousRows, namedRows);
}

TEST_F(AnonymousPatternPropertyTest, doesNotResolveAConstrainedAnonymousNodeByName) {
    expectError("MATCH (:Person {name: 'Remy'}) RETURN v0", "Variable 'v0' not found");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
