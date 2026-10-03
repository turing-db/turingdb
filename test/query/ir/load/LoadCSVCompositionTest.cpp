#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
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

namespace {

constexpr std::string_view PEOPLE_FILE = "Remy,red\n"
                                         "Adam,blue\n"
                                         "Nobody,green\n";

constexpr std::string_view HEADED_FILE = "who,colour\n"
                                         "Remy,red\n"
                                         "Adam,blue\n"
                                         "Nobody,green\n";

}

class LoadCSVCompositionTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        writeFile("people.csv", PEOPLE_FILE);
        writeFile("headed.csv", HEADED_FILE);
    }

protected:
    void writeFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
        file.close();
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::ranges::sort(sortedExpected);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sortedExpected) << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LoadCSVCompositionTest, optionalMatchAfterAMatchOnAField) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row MATCH (p:Person {name: row.who}) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f:Person) "
               "RETURN row.colour, f.name",
               {{"red", "Adam"}, {"blue", "Remy"}});
}

TEST_F(LoadCSVCompositionTest, matchAfterAnOptionalMatchOnAField) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row OPTIONAL MATCH (p:Person {name: row.who}) "
               "MATCH (i:Interest {name: 'Bio'}) "
               "RETURN row.who, p.name, i.name",
               {{"Remy", "Remy", "Bio"}, {"Adam", "Adam", "Bio"}, {"Nobody", "null", "Bio"}});
}

TEST_F(LoadCSVCompositionTest, optionalTraversalOutOfAField) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row "
               "OPTIONAL MATCH (p:Person {name: row.who})-[:INTERESTED_IN]->(i) "
               "RETURN row.who, i.name",
               {{"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Nobody", "null"}});
}

TEST_F(LoadCSVCompositionTest, twoOptionalMatchesOnFields) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row "
               "OPTIONAL MATCH (p:Person {name: row.who}) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f:Person) WHERE row.colour = 'red' "
               "RETURN row.who, p.name, f.name",
               {{"Remy", "Remy", "Adam"}, {"Adam", "Adam", "null"}, {"Nobody", "null", "null"}});
}

TEST_F(LoadCSVCompositionTest, aggregateGroupedOnAFieldAfterAnOptionalMatch) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row "
               "OPTIONAL MATCH (p:Person {name: row.who})-[:INTERESTED_IN]->(i) "
               "RETURN row.who, count(i)",
               {{"Remy", "3"}, {"Adam", "2"}, {"Nobody", "0"}});
}

TEST_F(LoadCSVCompositionTest, withAfterAnOptionalMatchCarriesAField) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row OPTIONAL MATCH (p:Person {name: row.who}) "
               "WITH row.colour AS colour, p WHERE p IS NULL "
               "RETURN colour",
               {{"green"}});
}

TEST_F(LoadCSVCompositionTest, orderedAndLimitedAfterAnOptionalMatch) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row OPTIONAL MATCH (p:Person {name: row.who}) "
               "RETURN row.who, p.name ORDER BY row.who LIMIT 2",
               {{"Adam", "Adam"}, {"Nobody", "null"}});
}

TEST_F(LoadCSVCompositionTest, optionalMatchAfterALoadCrossedWithAMatch) {
    expectRows("MATCH (p:Person {name: 'Remy'}) LOAD CSV 'headed.csv' WITH HEADERS AS row "
               "OPTIONAL MATCH (q:Person {name: row.who}) "
               "RETURN p.name, row.who, q.name",
               {{"Remy", "Remy", "Remy"}, {"Remy", "Adam", "Adam"}, {"Remy", "Nobody", "null"}});
}

TEST_F(LoadCSVCompositionTest, optionalMatchReadingTwoLoads) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS a LOAD CSV 'people.csv' AS b "
               "OPTIONAL MATCH (p:Person {name: a.who}) WHERE p.name = b[0] "
               "RETURN a.who, b[1], p.name",
               {{"Remy", "red", "Remy"},
                {"Remy", "blue", "null"},
                {"Remy", "green", "null"},
                {"Adam", "red", "null"},
                {"Adam", "blue", "Adam"},
                {"Adam", "green", "null"},
                {"Nobody", "red", "null"},
                {"Nobody", "blue", "null"},
                {"Nobody", "green", "null"}});
}

TEST_F(LoadCSVCompositionTest, unwindAfterALoad) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row UNWIND [1, 2] AS n "
               "OPTIONAL MATCH (p:Person {name: row.who}) "
               "RETURN row.who, n, p.name",
               {{"Remy", "1", "Remy"},
                {"Remy", "2", "Remy"},
                {"Adam", "1", "Adam"},
                {"Adam", "2", "Adam"},
                {"Nobody", "1", "null"},
                {"Nobody", "2", "null"}});
}

TEST_F(LoadCSVCompositionTest, uncorrelatedCallSubqueryAfterALoad) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row "
               "CALL { MATCH (p:Person) RETURN count(p) AS people } "
               "RETURN row.who, people",
               {{"Remy", "8"}, {"Adam", "8"}, {"Nobody", "8"}});
}

TEST_F(LoadCSVCompositionTest, callSubqueryImportingTheRow) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row "
               "CALL (row) { MATCH (p:Person {name: row.who})-[:INTERESTED_IN]->(i) RETURN count(i) AS interests } "
               "RETURN row.who, interests",
               {{"Remy", "3"}, {"Adam", "2"}, {"Nobody", "0"}});
}

TEST_F(LoadCSVCompositionTest, countSubqueryReadingAField) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row "
               "RETURN row.who, COUNT { MATCH (p:Person {name: row.who})-[:INTERESTED_IN]->() } AS interests",
               {{"Remy", "3"}, {"Adam", "2"}, {"Nobody", "0"}});
}

TEST_F(LoadCSVCompositionTest, patternComprehensionReadingAField) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row "
               "RETURN row.who, size([(p:Person {name: row.who})-[:INTERESTED_IN]->(i) | i.name]) AS interests",
               {{"Remy", "3"}, {"Adam", "2"}, {"Nobody", "0"}});
}

TEST_F(LoadCSVCompositionTest, listComprehensionReadingAField) {
    expectRows("LOAD CSV 'people.csv' AS row "
               "RETURN row[0], size([x IN [1, 2, 3] WHERE row[1] = 'red']) AS kept",
               {{"Remy", "3"}, {"Adam", "0"}, {"Nobody", "0"}});
}

TEST_F(LoadCSVCompositionTest, unionOfALoadAndAnOptionalMatchOnIt) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row RETURN row.colour AS name "
               "UNION "
               "LOAD CSV 'headed.csv' WITH HEADERS AS row OPTIONAL MATCH (p:Person {name: row.who}) "
               "RETURN p.name AS name",
               {{"red"}, {"blue"}, {"green"}, {"Remy"}, {"Adam"}, {"null"}});
}

TEST_F(LoadCSVCompositionTest, existsSubqueryReadingAField) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:KNOWS_WELL]->(:Person {name: row.who}) } "
               "RETURN row.who, p.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}});
}
