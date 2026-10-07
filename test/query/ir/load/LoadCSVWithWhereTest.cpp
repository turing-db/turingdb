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

constexpr std::string_view CONNECTIONS_FILE = "station1,station2,line\n"
                                              "A,B,Walk\n"
                                              "B,C,Bakerloo\n"
                                              "C,D,Walk\n"
                                              "D,E,Central\n";

constexpr std::string_view PEOPLE_FILE = "who,colour\n"
                                         "Remy,red\n"
                                         "Adam,blue\n"
                                         "Nobody,green\n";

constexpr std::string_view DUPLICATES_FILE = "who,colour\n"
                                             "Remy,red\n"
                                             "Remy,red\n"
                                             "Adam,blue\n";

}

class LoadCSVWithWhereTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        writeFile("connections.csv", CONNECTIONS_FILE);
        writeFile("people.csv", PEOPLE_FILE);
        writeFile("dups.csv", DUPLICATES_FILE);
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

TEST_F(LoadCSVWithWhereTest, countsTheRowsAFieldTheWithDroppedKeeps) {
    expectRows("LOAD CSV 'connections.csv' WITH HEADERS AS row "
               "WITH row.station1 AS s1 WHERE row.line = 'Walk' "
               "RETURN count(*)",
               {{"2"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldByPosition) {
    expectRows("LOAD CSV 'connections.csv' WITH HEADERS AS row "
               "WITH row.station1 AS s1 WHERE row[2] = 'Walk' "
               "RETURN count(*)",
               {{"2"}});
}

TEST_F(LoadCSVWithWhereTest, returnsTheProjectedFieldOfTheKeptRows) {
    expectRows("LOAD CSV 'connections.csv' WITH HEADERS AS row "
               "WITH row.station1 AS s1 WHERE row.line = 'Walk' "
               "RETURN s1",
               {{"A"}, {"C"}});
}

TEST_F(LoadCSVWithWhereTest, readsTheProjectionAndADroppedField) {
    expectRows("LOAD CSV 'connections.csv' WITH HEADERS AS row "
               "WITH row.station1 AS s1 WHERE s1 <> 'A' AND row.line = 'Walk' "
               "RETURN s1",
               {{"C"}});
}

TEST_F(LoadCSVWithWhereTest, readsAFieldTheProjectionAlsoReads) {
    expectRows("LOAD CSV 'connections.csv' WITH HEADERS AS row "
               "WITH row.station2 AS s2 WHERE row.station1 = 'B' OR row.station2 = 'E' "
               "RETURN s2",
               {{"C"}, {"E"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldWithoutHeaders) {
    expectRows("LOAD CSV 'connections.csv' AS row "
               "WITH row[0] AS s1 WHERE row[2] = 'Walk' "
               "RETURN s1",
               {{"A"}, {"C"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldAfterAMatch) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "MATCH (p:Person {name: row.who}) "
               "WITH p WHERE row.colour = 'blue' "
               "RETURN p.name",
               {{"Adam"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldInAnExistsSubquery) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "WITH row.colour AS colour WHERE EXISTS { MATCH (:Person {name: row.who}) } "
               "RETURN colour",
               {{"red"}, {"blue"}});
}

TEST_F(LoadCSVWithWhereTest, carriesTheRowThroughAWith) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "WITH row WHERE row.colour = 'red' "
               "RETURN row.who",
               {{"Remy"}});
}

TEST_F(LoadCSVWithWhereTest, carriesARowWithoutHeadersThroughAWith) {
    expectRows("LOAD CSV 'connections.csv' AS row "
               "WITH row WHERE row[2] = 'Walk' "
               "RETURN row[1]",
               {{"B"}, {"D"}});
}

TEST_F(LoadCSVWithWhereTest, countsTheRowsAWithCarriesWithNoFieldRead) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "WITH row "
               "RETURN count(*)",
               {{"3"}});
}

TEST_F(LoadCSVWithWhereTest, cutsTheRowsAWithCarries) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "WITH row ORDER BY row.who LIMIT 1 "
               "RETURN row.who, row.colour",
               {{"Adam", "blue"}});
}

TEST_F(LoadCSVWithWhereTest, projectsAFieldInACallBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "CALL (row) { WITH row.who AS who RETURN who } "
               "RETURN who",
               {{"Remy"}, {"Adam"}, {"Nobody"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldInACallBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "CALL (row) { WITH row.who AS who WHERE row.colour = 'red' RETURN who } "
               "RETURN who",
               {{"Remy"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldInACallBodyProjectingAConstant) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "CALL (row) { WITH 1 AS one WHERE row.colour = 'red' RETURN one } "
               "RETURN one",
               {{"1"}});
}

TEST_F(LoadCSVWithWhereTest, carriesTheRowThroughAWithInACallBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "CALL (row) { WITH row WHERE row.colour = 'red' RETURN row.who AS who } "
               "RETURN who",
               {{"Remy"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldInACallBodyImportingWithWith) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "CALL { WITH row WITH row.who AS who WHERE row.colour = 'red' RETURN who } "
               "RETURN who",
               {{"Remy"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldInAnOptionalCallBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "OPTIONAL CALL (row) { WITH row.who AS who WHERE row.colour = 'red' RETURN who } "
               "RETURN row.colour, who",
               {{"red", "Remy"}, {"blue", "null"}, {"green", "null"}});
}

TEST_F(LoadCSVWithWhereTest, carriesTheRowThroughAWithInAnOptionalCallBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "OPTIONAL CALL (row) { WITH row WHERE row.colour <> 'red' RETURN row.who AS who } "
               "RETURN row.colour, who",
               {{"red", "null"}, {"blue", "Adam"}, {"green", "Nobody"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldByPositionInACallBody) {
    expectRows("LOAD CSV 'connections.csv' AS row "
               "CALL (row) { WITH row[0] AS s1 WHERE row[2] = 'Walk' RETURN s1 } "
               "RETURN s1",
               {{"A"}, {"C"}});
}

TEST_F(LoadCSVWithWhereTest, carriesARowWithoutHeadersThroughAWithInACallBody) {
    expectRows("LOAD CSV 'connections.csv' AS row "
               "CALL (row) { WITH row WHERE row[2] = 'Walk' RETURN row[1] AS s2 } "
               "RETURN s2",
               {{"B"}, {"D"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldInAnExistsBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "MATCH (p:Person {name: row.who}) "
               "WHERE EXISTS { WITH 1 AS one WHERE row.colour = 'red' RETURN one } "
               "RETURN p.name",
               {{"Remy"}});
}

TEST_F(LoadCSVWithWhereTest, carriesTheRowThroughAWithInAnExistsBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "MATCH (p:Person {name: row.who}) "
               "WHERE EXISTS { WITH row WHERE row.colour = 'blue' RETURN row.who AS who } "
               "RETURN p.name",
               {{"Adam"}});
}

TEST_F(LoadCSVWithWhereTest, readsADroppedFieldInACountBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "RETURN row.who, COUNT { WITH row.who AS who WHERE row.colour = 'red' RETURN who }",
               {{"Remy", "1"}, {"Adam", "0"}, {"Nobody", "0"}});
}

TEST_F(LoadCSVWithWhereTest, groupsOneRowAtATimeInACallBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "CALL (row) { WITH row.who AS who, count(*) AS c RETURN who, c } "
               "RETURN who, c",
               {{"Remy", "1"}, {"Adam", "1"}, {"Nobody", "1"}});
}

TEST_F(LoadCSVWithWhereTest, groupsOneDuplicateRowAtATimeInACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { WITH row.who AS who, count(*) AS c RETURN who, c } "
               "RETURN who, c",
               {{"Remy", "1"}, {"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(LoadCSVWithWhereTest, groupsAndReadsADroppedFieldInACallBody) {
    expectRows("LOAD CSV 'people.csv' WITH HEADERS AS row "
               "CALL (row) { WITH row.who AS who, count(*) AS c WHERE row.colour = 'red' RETURN who, c } "
               "RETURN who, c",
               {{"Remy", "1"}});
}

TEST_F(LoadCSVWithWhereTest, countsOneRowAtATimeInACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { WITH count(*) AS c RETURN c } "
               "RETURN row.who, c",
               {{"Remy", "1"}, {"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(LoadCSVWithWhereTest, collectsOneRowAtATimeInACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { WITH collect(row.who) AS whos RETURN whos } "
               "RETURN whos",
               {{"Remy"}, {"Remy"}, {"Adam"}});
}

TEST_F(LoadCSVWithWhereTest, groupsInTheReturnOfACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { RETURN row.who AS who, count(*) AS c } "
               "RETURN who, c",
               {{"Remy", "1"}, {"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(LoadCSVWithWhereTest, dedupsOneRowAtATimeInACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { WITH DISTINCT row.colour AS c RETURN c } "
               "RETURN c",
               {{"red"}, {"red"}, {"blue"}});
}

TEST_F(LoadCSVWithWhereTest, dedupsTheRowInACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { WITH DISTINCT row RETURN row.who AS who } "
               "RETURN who",
               {{"Remy"}, {"Remy"}, {"Adam"}});
}

TEST_F(LoadCSVWithWhereTest, groupsTheRowInACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { UNWIND [1, 2] AS x WITH row, count(x) AS c RETURN row.who AS who, c } "
               "RETURN who, c",
               {{"Remy", "2"}, {"Remy", "2"}, {"Adam", "2"}});
}

TEST_F(LoadCSVWithWhereTest, groupsTwiceInACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { "
               "  UNWIND [1, 1, 2] AS x WITH x, count(*) AS n "
               "  WITH n, count(*) AS c WHERE row.colour = 'red' "
               "  RETURN n, c "
               "} "
               "RETURN row.who, n, c",
               {{"Remy", "1", "1"}, {"Remy", "2", "1"}, {"Remy", "1", "1"}, {"Remy", "2", "1"}});
}

TEST_F(LoadCSVWithWhereTest, dedupsAndReadsADroppedFieldInACallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { WITH DISTINCT row.who AS who WHERE row.colour = 'red' RETURN who } "
               "RETURN who",
               {{"Remy"}, {"Remy"}});
}

TEST_F(LoadCSVWithWhereTest, dedupsInAnOptionalCallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "OPTIONAL CALL (row) { WITH DISTINCT row.colour AS c WHERE row.who = 'Adam' RETURN c } "
               "RETURN row.who, c",
               {{"Remy", "null"}, {"Remy", "null"}, {"Adam", "blue"}});
}

TEST_F(LoadCSVWithWhereTest, groupsInAnOptionalCallBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "OPTIONAL CALL (row) { WITH row.colour AS colour, count(*) AS c WHERE row.who = 'Remy' RETURN colour, c } "
               "RETURN row.who, colour, c",
               {{"Remy", "red", "1"}, {"Remy", "red", "1"}, {"Adam", "null", "null"}});
}

TEST_F(LoadCSVWithWhereTest, groupsARowWithoutHeadersInACallBody) {
    expectRows("LOAD CSV 'connections.csv' AS row "
               "CALL (row) { WITH row[2] AS line, count(*) AS c RETURN line, c } "
               "RETURN line, c",
               {{"line", "1"}, {"Walk", "1"}, {"Bakerloo", "1"}, {"Walk", "1"}, {"Central", "1"}});
}

TEST_F(LoadCSVWithWhereTest, dedupsARowWithoutHeadersInACallBody) {
    expectRows("LOAD CSV 'connections.csv' AS row "
               "CALL (row) { WITH DISTINCT row[2] AS line WHERE row[0] <> 'station1' RETURN line } "
               "RETURN line",
               {{"Walk"}, {"Bakerloo"}, {"Walk"}, {"Central"}});
}

TEST_F(LoadCSVWithWhereTest, groupsOneRowAtATimeInACountBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "RETURN row.who, COUNT { WITH row.colour AS colour, count(*) AS c RETURN colour }",
               {{"Remy", "1"}, {"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(LoadCSVWithWhereTest, dedupsOneRowAtATimeInACountBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "RETURN row.who, COUNT { WITH DISTINCT row.colour AS c WHERE row.who = 'Remy' RETURN c }",
               {{"Remy", "1"}, {"Remy", "1"}, {"Adam", "0"}});
}

TEST_F(LoadCSVWithWhereTest, dedupsOneRowAtATimeInAnExistsBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "WITH row.who AS who WHERE EXISTS { WITH DISTINCT row.colour AS c WHERE c = 'red' RETURN c } "
               "RETURN who",
               {{"Remy"}, {"Remy"}});
}

TEST_F(LoadCSVWithWhereTest, groupsOneRowAtATimeInAnExistsBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "WITH row.who AS who WHERE EXISTS { WITH row.colour AS c, count(*) AS n WHERE c = 'blue' RETURN c } "
               "RETURN who",
               {{"Adam"}});
}

TEST_F(LoadCSVWithWhereTest, groupsAndDedupsInTheBranchesOfAUnionBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "CALL (row) { "
               "  WITH row.who AS value, count(*) AS c RETURN value, c "
               "  UNION "
               "  WITH DISTINCT row.colour AS value RETURN value, 0 AS c "
               "} "
               "RETURN value, c",
               {{"Remy", "1"}, {"Remy", "1"}, {"Adam", "1"}, {"red", "0"}, {"red", "0"}, {"blue", "0"}});
}

TEST_F(LoadCSVWithWhereTest, groupsARowWithoutHeadersByItselfInACallBody) {
    expectRows("LOAD CSV 'dups.csv' AS row "
               "CALL (row) { WITH row, count(*) AS c RETURN row[1] AS colour, c } "
               "RETURN colour, c",
               {{"colour", "1"}, {"red", "1"}, {"red", "1"}, {"blue", "1"}});
}

TEST_F(LoadCSVWithWhereTest, groupsTheRowInAnExistsBody) {
    expectRows("LOAD CSV 'dups.csv' WITH HEADERS AS row "
               "WITH row.who AS who WHERE EXISTS { WITH row, count(*) AS c WHERE row.colour = 'red' RETURN c } "
               "RETURN who",
               {{"Remy"}, {"Remy"}});
}
