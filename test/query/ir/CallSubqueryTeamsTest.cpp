#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "NLOutputSink.h"
#include "QueryConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// What a CALL subquery asks for per input row where the clauses around it would ask over
// every row at once: an ordering and a cut, an aggregate that another pattern must not
// multiply, and a number of writes. Each one is paired with what the same query says
// without the subquery, which is the answer the engine gave before it had one.
class CallSubqueryTeamsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    // Three teams and six players, one of whom plays for none. Team A owes two debts of
    // 1500 and one of 3000: the equal pair is what tells a sum of the distinct amounts
    // apart from a sum of the debts.
    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        std::vector<NodeID> teams;
        for (const std::string_view name : {"Team A", "Team B", "Team C"}) {
            const NodeID team = writer.addNode({"Team"});
            writer.addNodeProperty<types::String>(team, "name", std::string_view {name});
            teams.push_back(team);
        }

        const std::vector<std::pair<std::string_view, int64_t>> roster {{"Player A", 21},
                                                                       {"Player B", 23},
                                                                       {"Player C", 19},
                                                                       {"Player D", 30},
                                                                       {"Player E", 25},
                                                                       {"Player F", 35}};

        std::vector<NodeID> players;
        for (const auto& [name, age] : roster) {
            const NodeID player = writer.addNode({"Player"});
            writer.addNodeProperty<types::String>(player, "name", std::string_view {name});
            writer.addNodeProperty<types::Int64>(player, "age", int64_t {age});
            players.push_back(player);
        }

        writer.addEdge("PLAYS_FOR", players[0], teams[0]);
        writer.addEdge("PLAYS_FOR", players[1], teams[0]);
        writer.addEdge("PLAYS_FOR", players[3], teams[1]);
        writer.addEdge("PLAYS_FOR", players[4], teams[2]);
        writer.addEdge("PLAYS_FOR", players[5], teams[2]);

        const auto owes = [&](size_t from, size_t to, int64_t dollars) {
            const EdgeRecord edge = writer.addEdge("OWES", teams[from], teams[to]);
            writer.addEdgeProperty<types::Int64>(edge, "dollars", std::move(dollars));
        };

        owes(0, 1, 1500);
        owes(0, 1, 3000);
        owes(0, 1, 1500);
        owes(1, 2, 1700);
        owes(2, 1, 5000);

        writer.submit();
        jobSystem.terminate();
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
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

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    // The rows a writing query emits, in its own change, submitted so a following read
    // sees what it wrote
    void expectWriteRows(std::string_view query, const Rows& expected) {
        // Released before the query runs: it takes the system for itself
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto opened = system.newChange(_graphName);
            ASSERT_TRUE(opened);

            changeID = opened.value()->id();
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

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);

        const QueryStatus submitted = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitted.isOk()) << "CHANGE SUBMIT failed";
    }

    const std::string _graphName = "teams";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

// The cut reads the rows of one team, so every team keeps its oldest player
TEST_F(CallSubqueryTeamsTest, takesTheOldestPlayerOfEachTeam) {
    expectRowsInOrder("MATCH (t:Team) "
                      "CALL (t) { "
                      "  MATCH (p:Player)-[:PLAYS_FOR]->(t) "
                      "  RETURN p.name AS player, p.age AS age ORDER BY age DESC LIMIT 1 "
                      "} "
                      "RETURN t.name AS team, player, age ORDER BY team",
                      {{"Team A", "Player B", "23"},
                       {"Team B", "Player D", "30"},
                       {"Team C", "Player F", "35"}});
}

// The same cut outside the subquery reads every matched row at once, so the whole match
// keeps a single player - the oldest of them all
TEST_F(CallSubqueryTeamsTest, aCutOutsideTheSubqueryKeepsOnePlayerForTheWholeMatch) {
    expectRowsInOrder("MATCH (t:Team)<-[:PLAYS_FOR]-(p:Player) "
                      "WITH t, p ORDER BY p.age DESC LIMIT 1 "
                      "RETURN t.name AS team, p.name AS player, p.age AS age",
                      {{"Team C", "Player F", "35"}});
}

// Each subquery reads the rows of its own pattern, so neither aggregate counts the rows of
// the other: Team A fields two players and owes 6000 over three debts
TEST_F(CallSubqueryTeamsTest, countsPlayersAndSumsDebtsIndependently) {
    expectRowsInOrder("MATCH (t:Team) "
                      "CALL (t) { MATCH (p:Player)-[:PLAYS_FOR]->(t) RETURN count(p) AS players } "
                      "CALL (t) { MATCH (t)-[o:OWES]->(:Team) RETURN sum(o.dollars) AS owed } "
                      "RETURN t.name AS team, players, owed ORDER BY team",
                      {{"Team A", "2", "6000"},
                       {"Team B", "1", "1700"},
                       {"Team C", "2", "5000"}});
}

// Both patterns hang off the same team, so their rows multiply: Team A's two players meet
// its three debts over six rows, and each aggregate reads all six
TEST_F(CallSubqueryTeamsTest, twoOptionalPatternsMultiplyEachOthersRows) {
    expectRowsInOrder("MATCH (t:Team) "
                      "OPTIONAL MATCH (p:Player)-[:PLAYS_FOR]->(t) "
                      "OPTIONAL MATCH (t)-[o:OWES]->(:Team) "
                      "RETURN t.name AS team, count(p) AS players, sum(o.dollars) AS owed ORDER BY team",
                      {{"Team A", "6", "12000"},
                       {"Team B", "1", "1700"},
                       {"Team C", "2", "10000"}});
}

// DISTINCT drops the repeated rows, which answers for the count and not for the sum: Team
// A's two debts of 1500 are one amount, so 1500 of what it owes goes unreported
TEST_F(CallSubqueryTeamsTest, distinctRepairsTheCountAndStillBreaksTheSum) {
    expectRowsInOrder("MATCH (t:Team) "
                      "OPTIONAL MATCH (p:Player)-[:PLAYS_FOR]->(t) "
                      "OPTIONAL MATCH (t)-[o:OWES]->(:Team) "
                      "RETURN t.name AS team, count(DISTINCT p) AS players, sum(DISTINCT o.dollars) AS owed "
                      "ORDER BY team",
                      {{"Team A", "2", "4500"},
                       {"Team B", "1", "1700"},
                       {"Team C", "2", "5000"}});
}

// The body writes three times for each player and hands back the row it was given, so the
// query still reports the six players it matched
TEST_F(CallSubqueryTeamsTest, writesThreeTimesPerPlayerAndKeepsThePlayerRows) {
    expectWriteRows("MATCH (p:Player) "
                    "CALL (p) { UNWIND [1, 2, 3] AS copy CREATE (:Copy {name: p.name}) } "
                    "RETURN count(p) AS players",
                    {{"6"}});

    expectRowsInOrder("MATCH (c:Copy) RETURN count(c) AS copies", {{"18"}});
}

// The same three writes without a subquery: the UNWIND that drives them multiplies the
// rows the query goes on to report
TEST_F(CallSubqueryTeamsTest, theSameWritesWithoutASubqueryMultiplyTheRows) {
    expectRowsInOrder("MATCH (p:Player) UNWIND [1, 2, 3] AS copy RETURN count(p) AS players",
                      {{"18"}});
}

// The Neo4j manual's post-union processing: the youngest and the oldest player
TEST_F(CallSubqueryTeamsTest, ordersAfterAUnionOfTwoCuts) {
    expectRowsInOrder("CALL () { "
                      "  MATCH (p:Player) RETURN p ORDER BY p.age ASC LIMIT 1 "
                      "  UNION "
                      "  MATCH (p:Player) RETURN p ORDER BY p.age DESC LIMIT 1 "
                      "} "
                      "RETURN p.name AS playerName, p.age AS age ORDER BY age",
                      {{"Player C", "19"}, {"Player F", "35"}});
}

// The UNION dedups per team: Team A's two debts of 1500 count once, and so do the two
// 1500 Team B is owed, so A sums -4500 rather than -6000 and B 7800 rather than 9300
TEST_F(CallSubqueryTeamsTest, sumsTheDistinctDebtsOfEachTeam) {
    expectRowsInOrder("MATCH (t:Team) "
                      "CALL (t) { "
                      "  OPTIONAL MATCH (t)-[o:OWES]->(other:Team) "
                      "  RETURN o.dollars * -1 AS moneyOwed "
                      "  UNION "
                      "  OPTIONAL MATCH (other)-[o:OWES]->(t) "
                      "  RETURN o.dollars AS moneyOwed "
                      "} "
                      "RETURN t.name AS team, sum(moneyOwed) AS amountOwed ORDER BY amountOwed DESC",
                      {{"Team B", "7800"}, {"Team C", "-3300"}, {"Team A", "-4500"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
