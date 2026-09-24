#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "iterators/ChunkConfig.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// `[(a)-[:KNOWS]->(b) WHERE p(b) | f(b)]` matches a pattern once per row in flight and
// builds one list per row out of what its matches contribute, whatever the pattern walks
// and whatever the body reads beside it.
class PatternComprehensionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    // Writes in a change of its own and submits it, so the read that follows sees a
    // committed property rather than the column the writing query happened to build - the
    // shape ListComprehensionTest uses.
    void write(std::string_view query) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto res = system.newChange(_graphName);
            ASSERT_TRUE(res);

            changeID = res.value()->id();
        }

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "CHANGE SUBMIT failed";
    }

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(PatternComprehensionTest, projectsEveryMatch) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-[:INTERESTED_IN]->(i) | i.name]",
               {{"[Ghosts, Computers, Eighties]"}});
}

TEST_F(PatternComprehensionTest, buildsTheEmptyListWhereNothingMatches) {
    expectRows("MATCH (n:Person {name: 'Suhas'}) RETURN [(n)-[:KNOWS_WELL]->(b) | b.name]",
               {{"[]"}});
}

TEST_F(PatternComprehensionTest, buildsOneListPerRow) {
    expectRows("MATCH (n:Person) RETURN n.name, [(n)-[:INTERESTED_IN]->(i) | i.name]",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[Cooking]"},
                {"Suhas", "[Gym, JiuJitsu]"},
                {"Cyrus", "[Gym, Travel]"},
                {"Doruk", "[Gym]"}});
}

TEST_F(PatternComprehensionTest, filtersItsMatches) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN [(n)-[:INTERESTED_IN]->(i) WHERE i.isReal = true | i.name]",
               {{"[Ghosts, Computers]"}});
}

TEST_F(PatternComprehensionTest, constrainsItsPatternByAProperty) {
    expectRows("MATCH (n:Person {name: 'Adam'}) "
               "RETURN [(n)-[:INTERESTED_IN]->(i {name: 'Bio'}) | i.name]",
               {{"[Bio]"}});
}

TEST_F(PatternComprehensionTest, readsTheEdgeItWalked) {
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN [(n)-[e:INTERESTED_IN]->(i) | e.name]",
               {{"[Adam -> Bio, Adam -> Cooking]"}});
}

TEST_F(PatternComprehensionTest, walksTwoHops) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN [(n)-[:KNOWS_WELL]->(m)-[:INTERESTED_IN]->(i) | i.name]",
               {{"[Bio, Cooking]"}});
}

TEST_F(PatternComprehensionTest, walksAnUndirectedHop) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-[:KNOWS_WELL]-(b) | b.name]",
               {{"[Adam, Adam, Ghosts]"}});
}

TEST_F(PatternComprehensionTest, walksAnIncomingHop) {
    expectRows("MATCH (i:Interest {name: 'Gym'}) RETURN [(i)<-[:INTERESTED_IN]-(p) | p.name]",
               {{"[Cyrus, Suhas, Doruk]"}});
}

TEST_F(PatternComprehensionTest, walksAHopOfAnyType) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-->(x) | x.name]",
               {{"[Adam, Ghosts, Computers, Eighties]"}});
}

TEST_F(PatternComprehensionTest, readsTheRowItIsReadOn) {
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN [(n)-[:INTERESTED_IN]->(i) | n.name]",
               {{"[Adam, Adam]"}});
}

TEST_F(PatternComprehensionTest, projectsAConstant) {
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN [(n)-[:INTERESTED_IN]->(i) | 1]",
               {{"[1, 1]"}});
}

TEST_F(PatternComprehensionTest, projectsTheNodeItMatched) {
    expectRows("MATCH (n:Person {name: 'Martina'}) RETURN [(n)-[:INTERESTED_IN]->(i) | i]",
               {{"[5]"}});
}

TEST_F(PatternComprehensionTest, matchesTheWholeGraphWhereNothingIsInFlight) {
    expectRows("RETURN [(a:Person)-[:KNOWS_WELL]->(b:Person) | b.name]", {{"[Adam, Remy]"}});
}

TEST_F(PatternComprehensionTest, joinsTwoVariablesAlreadyInFlight) {
    expectRows("MATCH (n:Person {name: 'Remy'}), (m:Person {name: 'Adam'}) "
               "RETURN [(n)-[e]->(m) | e.name]",
               {{"[Remy -> Adam]"}});
}

TEST_F(PatternComprehensionTest, buildsAListPerFactorOfACrossProduct) {
    expectRows("MATCH (n:Person {name: 'Adam'}), (m:Person {name: 'Luc'}) "
               "RETURN [(n)-[:INTERESTED_IN]->(i) | i.name], [(m)-[:INTERESTED_IN]->(j) | j.name]",
               {{"[Bio, Cooking]", "[Animals, Computers]"}});
}

TEST_F(PatternComprehensionTest, buildsItsListsBesideAnAnonymousHop) {
    expectRows("MATCH (n:Person {name: 'Remy'})-[:INTERESTED_IN]->() "
               "RETURN [(n)-[:KNOWS_WELL]->(b) | b.name]",
               {{"[Adam]"}, {"[Adam]"}, {"[Adam]"}});
}

TEST_F(PatternComprehensionTest, walksFromAColumnAHopBound) {
    expectRows("MATCH (n:Person {name: 'Remy'})-[:KNOWS_WELL]->(m) "
               "RETURN m.name, [(m)-[:INTERESTED_IN]->(i) | i.name]",
               {{"Adam", "[Bio, Cooking]"}});
}

TEST_F(PatternComprehensionTest, readsTheRowsAnOptionalMatchPadded) {
    expectRows("MATCH (n:Person {name: 'Suhas'}) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(f) "
               "RETURN f, [(n)-[:INTERESTED_IN]->(i) | i.name]",
               {{"null", "[Gym, JiuJitsu]"}});
}

TEST_F(PatternComprehensionTest, buildsItsListsInsideASubquery) {
    expectRows("MATCH (n:Person {name: 'Adam'}) "
               "CALL { WITH n RETURN [(n)-[:INTERESTED_IN]->(i) | i.name] AS l } "
               "RETURN l",
               {{"[Bio, Cooking]"}});
}

TEST_F(PatternComprehensionTest, buildsTwoListsInOneProjection) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN [(n)-[:INTERESTED_IN]->(i) | i.name], [(n)-[:KNOWS_WELL]->(b) | b.name]",
               {{"[Ghosts, Computers, Eighties]", "[Adam]"}});
}

TEST_F(PatternComprehensionTest, nestsOneComprehensionInAnother) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN [(n)-[:KNOWS_WELL]->(m) | [(m)-[:INTERESTED_IN]->(i) | i.name]]",
               {{"[[Bio, Cooking]]"}});
}

TEST_F(PatternComprehensionTest, readsItInsideAListComprehension) {
    expectRows("MATCH (n:Person {name: 'Adam'}) "
               "RETURN [x IN [1,2] | size([(n)-[:INTERESTED_IN]->(i) | i])]",
               {{"[2, 2]"}});
}

TEST_F(PatternComprehensionTest, sizesItsResult) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN size([(n)-[:INTERESTED_IN]->(i) | i])",
               {{"3"}});
}

TEST_F(PatternComprehensionTest, filtersRowsBySizeOfItsResult) {
    expectRows("MATCH (n:Person) WHERE size([(n)-[:INTERESTED_IN]->(i) | i]) > 2 RETURN n.name",
               {{"Remy"}});
}

TEST_F(PatternComprehensionTest, ordersRowsBySizeOfItsResult) {
    expectRows("MATCH (n:Person) RETURN n.name "
               "ORDER BY size([(n)-[:INTERESTED_IN]->(i) | i]) DESC LIMIT 1",
               {{"Remy"}});
}

TEST_F(PatternComprehensionTest, aggregatesTheSizesItBuilt) {
    expectRows("MATCH (n:Person) RETURN sum(size([(n)-[:INTERESTED_IN]->(i) | i]))", {{"15"}});
}

TEST_F(PatternComprehensionTest, groupsTheRowsByTheListItBuilt) {
    expectRows("MATCH (n:Person) RETURN [(n)-[:KNOWS_WELL]->(b) | b.name] AS known, count(*)",
               {{"[Adam]", "1"}, {"[Remy]", "1"}, {"[]", "6"}});
}

TEST_F(PatternComprehensionTest, unwindsItsResult) {
    expectRows("MATCH (n:Person {name: 'Adam'}) "
               "UNWIND [(n)-[:INTERESTED_IN]->(i) | i.name] AS interest "
               "RETURN interest",
               {{"Bio"}, {"Cooking"}});
}

TEST_F(PatternComprehensionTest, carriesItsListPastAWithBarrier) {
    expectRows("MATCH (n:Person {name: 'Adam'}) WITH n, [(n)-[:INTERESTED_IN]->(i) | i.name] AS l "
               "RETURN l, size(l)",
               {{"[Bio, Cooking]", "2"}});
}

TEST_F(PatternComprehensionTest, filtersAWithByTheListItPublished) {
    expectRows("MATCH (n:Person) WITH n.name AS name, [(n)-[:INTERESTED_IN]->(i) | i.name] AS l "
               "WHERE size(l) > 2 RETURN name, l",
               {{"Remy", "[Ghosts, Computers, Eighties]"}});
}

TEST_F(PatternComprehensionTest, carriesItsListThroughAHopBelowTheBarrier) {
    expectRows("MATCH (n:Person {name: 'Adam'}) WITH n, [(n)-[:INTERESTED_IN]->(i) | i.name] AS l "
               "MATCH (n)-[:KNOWS_WELL]->(f) "
               "RETURN f.name, l",
               {{"Remy", "[Bio, Cooking]"}});
}

TEST_F(PatternComprehensionTest, ordersTheRowsItBuiltListsFor) {
    expectRows("MATCH (n:Person) WITH n.name AS name, [(n)-[:INTERESTED_IN]->(i) | i.name] AS l "
               "RETURN name, l ORDER BY name LIMIT 2",
               {{"Adam", "[Bio, Cooking]"},
                {"Cyrus", "[Gym, Travel]"}});
}

TEST_F(PatternComprehensionTest, writesItsListToAProperty) {
    write("MATCH (n:Person {name: 'Adam'}) "
          "SET n.interests = [(n)-[:INTERESTED_IN]->(i) | i.name]");

    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN n.interests", {{"[Bio, Cooking]"}});
}

// The accumulator covers one step of the rows in flight, so a chunk that ends mid-row set
// must leave the lists of the rows behind it alone.
TEST_F(PatternComprehensionTest, buildsItsListsOneStepOfTheRowsAtATime) {
    const std::vector<size_t> chunkSizes {1, 2, 3, 5, ChunkConfig::CHUNK_SIZE};

    for (const size_t chunkSize : chunkSizes) {
        _interpreter->setChunkSize(chunkSize);

        expectRows("MATCH (n:Person) RETURN n.name, [(n)-[:INTERESTED_IN]->(i) | i.name]",
                   {{"Remy", "[Ghosts, Computers, Eighties]"},
                    {"Adam", "[Bio, Cooking]"},
                    {"Maxime", "[Bio, Padel]"},
                    {"Luc", "[Animals, Computers]"},
                    {"Martina", "[Cooking]"},
                    {"Suhas", "[Gym, JiuJitsu]"},
                    {"Cyrus", "[Gym, Travel]"},
                    {"Doruk", "[Gym]"}});
    }
}

// Remy has three interests and no other person has more, so the descending order puts
// that group first whatever the ties behind it do.
TEST_F(PatternComprehensionTest, ordersTheGroupsByTheListItBuilt) {
    expectRows("MATCH (n:Person) RETURN n, count(*) "
               "ORDER BY size([(n)-[:INTERESTED_IN]->(i) | i]) DESC LIMIT 1",
               {{"0", "1"}});
}

// The hop leaves 15 rows in flight and the grouping key leaves 8 groups, so the rows the
// pattern is matched on are the groups and not what the aggregate consumed.
TEST_F(PatternComprehensionTest, buildsItsListsOverTheGroupsAnAggregateLeft) {
    expectRows("MATCH (n:Person)-[:INTERESTED_IN]->(x) RETURN n, count(*) "
               "ORDER BY size([(n)-[:KNOWS_WELL]->(b) | b])",
               {{"0", "3"},
                {"1", "2"},
                {"8", "2"},
                {"9", "2"},
                {"11", "1"},
                {"12", "2"},
                {"15", "2"},
                {"17", "1"}});
}

TEST_F(PatternComprehensionTest, ordersAnAggregatingProjectionByAPatternOverAConsumedVariable) {
    expectError("MATCH (n:Person)-[:KNOWS_WELL]->(b) RETURN n, count(*) "
                "ORDER BY size([(b)-[:INTERESTED_IN]->(i) | i])",
                "ORDER BY with an aggregate may only order by expressions over the returned columns");
}

TEST_F(PatternComprehensionTest, ordersAnAggregatingProjectionByAPatternOverAGroupedProperty) {
    expectError("MATCH (n:Person) RETURN n.name, count(*) "
                "ORDER BY size([(n)-[:INTERESTED_IN]->(i) | i])",
                "ORDER BY with an aggregate may only order by expressions over the returned columns");
}

TEST_F(PatternComprehensionTest, aggregatesOverItsMatches) {
    expectError("MATCH (n:Person) RETURN [(n)-[:INTERESTED_IN]->(i) | count(i)]",
                "Aggregate functions may not be used over the matches");
}

TEST_F(PatternComprehensionTest, dropsItsVariablesAfterTheBody) {
    expectError("MATCH (n:Person) RETURN [(n)-[:INTERESTED_IN]->(i) | i.name], i.name",
                "not found");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
