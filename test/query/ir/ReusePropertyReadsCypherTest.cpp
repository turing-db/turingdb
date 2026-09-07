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

// The query shapes that read one property twice over the same rows, run on simpledb: the
// rows have to come out as they did when each read fetched the property again.
class ReusePropertyReadsCypherTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRows(std::string_view query, std::vector<StringRowSink::Row> expected) {
        StringRowSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Remy and Adam are the only nodes with an age, both 32. Codegen splits the conjunction
// into a filter per conjunct, so the age is read once per filter and once for the RETURN.
TEST_F(ReusePropertyReadsCypherTest, readsTheAgeOfARangeOnBothSides) {
    expectRows("MATCH (n) WHERE n.age > 20 AND n.age < 50 RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(ReusePropertyReadsCypherTest, returnsTheAgeItFilteredOn) {
    expectRows("MATCH (n) WHERE n.age > 20 RETURN n.age, n.age + 1", {{"32", "33"}, {"32", "33"}});
}

// The Remy <-> Adam pair and Remy's INTERESTED_IN edges last 20, Luc -> Animals 20 and
// Luc -> Computers 15; Ghosts -> Remy lasts 200.
TEST_F(ReusePropertyReadsCypherTest, readsAnEdgePropertyOnBothSidesOfARange) {
    expectRows("MATCH (a)-[e]->(b) WHERE e.duration > 15 AND e.duration < 100 RETURN e.name",
               {{"Remy -> Adam"}, {"Remy -> Ghosts"}, {"Remy -> Eighties"},
                {"Adam -> Remy"}, {"Luc -> Animals"}});
}

TEST_F(ReusePropertyReadsCypherTest, ordersByTheNameItReturns) {
    expectRows("MATCH (p:Person) RETURN p.name ORDER BY p.name",
               {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
}

TEST_F(ReusePropertyReadsCypherTest, cutsTheRowsItOrderedByAName) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name SKIP 1 LIMIT 3 RETURN p.name",
               {{"Cyrus"}, {"Doruk"}, {"Luc"}});
}

// Remy has four edges out and Adam three, so the age the filter read has to reach the
// projection through the hop that expanded those rows.
TEST_F(ReusePropertyReadsCypherTest, readsTheSourceAgeAcrossAHop) {
    expectRows("MATCH (a)-[e]->(b) WHERE a.age > 20 RETURN a.age, e.name, b.name",
               {{"32", "Remy -> Adam", "Adam"},
                {"32", "Remy -> Ghosts", "Ghosts"},
                {"32", "Remy -> Computers", "Computers"},
                {"32", "Remy -> Eighties", "Eighties"},
                {"32", "Adam -> Remy", "Remy"},
                {"32", "Adam -> Bio", "Bio"},
                {"32", "Adam -> Cooking", "Cooking"}});
}

// The name of the source is read after two hops, past a filter on a property of its own.
TEST_F(ReusePropertyReadsCypherTest, readsTheSourceNameAcrossTwoHops) {
    expectRows("MATCH (a)-[e]->(b)-->(c) WHERE a.age > 20 RETURN a.name, c.name",
               {{"Remy", "Remy"}, {"Remy", "Bio"}, {"Remy", "Cooking"}, {"Remy", "Remy"},
                {"Adam", "Adam"}, {"Adam", "Ghosts"}, {"Adam", "Computers"}, {"Adam", "Eighties"}});
}

// The target name is filtered on and returned, while the source age rides the same hop.
TEST_F(ReusePropertyReadsCypherTest, readsBothEndsOfAHopTwice) {
    expectRows("MATCH (a)-->(b) WHERE a.age > 20 AND b.name <> 'Bio' RETURN a.age, b.name",
               {{"32", "Adam"}, {"32", "Ghosts"}, {"32", "Computers"},
                {"32", "Eighties"}, {"32", "Remy"}, {"32", "Cooking"}});
}
