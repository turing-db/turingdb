#include <gtest/gtest.h>

#include <stddef.h>
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

// Three names simpledb carries and one it does not, so a match against the file keeps
// only the records the graph answers.
constexpr std::string_view peopleFile = "Remy,red\n"
                                        "Adam,blue\n"
                                        "Nobody,green\n";

constexpr std::string_view headedFile = "who,colour\n"
                                        "Remy,red\n"
                                        "Adam,blue\n";

// simpledb carries eight Person nodes, which is the left factor of a product a load is
// crossed into.
constexpr size_t simpledbPersonCount = 8;

}

// LOAD CSV composed with a MATCH: the load reads no column of the graph, so its records
// are crossed with what the pattern matched and the predicates cut the pairs - a file
// joined against the graph on whatever the query compares.
class LoadCSVMatchTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        writeFile("people.csv", peopleFile);
        writeFile("headed.csv", headedFile);
    }

protected:
    void writeFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
        file.close();
    }

    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }

    // The rows a crossed query reports follow the order the product built them in, which
    // is the traversal's rather than the file's, so they are compared as a set.
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

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

TEST_F(LoadCSVMatchTest, joinsAFieldAgainstANodePropertyInAWhere) {
    expectRows("LOAD CSV 'people.csv' AS row MATCH (n:Person) WHERE n.name = row[0] "
               "RETURN n.name AS name, row[1] AS colour",
               {{"Remy", "red"}, {"Adam", "blue"}});
}

// The predicate reads the same either way round, so which side of it the query writes the
// field on is its own choice.
TEST_F(LoadCSVMatchTest, joinsWithTheFieldOnTheLeftOfThePredicate) {
    expectRows("LOAD CSV 'people.csv' AS row MATCH (n:Person) WHERE row[0] = n.name "
               "RETURN n.name AS name, row[1] AS colour",
               {{"Remy", "red"}, {"Adam", "blue"}});
}

// A property constraint in the pattern is the same join written inline.
TEST_F(LoadCSVMatchTest, joinsAFieldThroughAPatternConstraint) {
    expectRows("LOAD CSV 'people.csv' AS row MATCH (n:Person {name: row[0]}) "
               "RETURN n.name AS name, row[1] AS colour",
               {{"Remy", "red"}, {"Adam", "blue"}});
}

TEST_F(LoadCSVMatchTest, joinsAHeaderFieldAgainstANodeProperty) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row MATCH (n:Person {name: row.who}) "
               "RETURN n.name AS name, row.colour AS colour",
               {{"Remy", "red"}, {"Adam", "blue"}});
}

// The row the file matched is a node like any other, so a pattern walks out of it.
TEST_F(LoadCSVMatchTest, walksATraversalOutOfTheMatchedRecord) {
    expectRows("LOAD CSV 'people.csv' AS row MATCH (n {name: row[0]})-[:INTERESTED_IN]->(i) "
               "RETURN row[0] AS who, i.name AS interest",
               {{"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"}});
}

// A load naming no variable of the pattern is crossed with it whichever order the query
// writes the two clauses in.
TEST_F(LoadCSVMatchTest, crossesTheRecordsWithAMatchWrittenFirst) {
    QueryStatus status;
    StringRowSink sink;
    runQuery("MATCH (n:Person) LOAD CSV 'people.csv' AS row RETURN n.name, row[0]", status, sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getRows().size(), simpledbPersonCount * 3);
}

TEST_F(LoadCSVMatchTest, reducesTheJoinedRowsToAnAggregate) {
    expectRows("LOAD CSV 'people.csv' AS row MATCH (n:Person {name: row[0]}) "
               "RETURN count(*) AS matched",
               {{"2"}});
}

// A WITH closes the join and publishes its columns, so what follows reads the matched
// pairs rather than the file.
TEST_F(LoadCSVMatchTest, carriesTheJoinedRowsPastAWith) {
    expectRows("LOAD CSV 'people.csv' AS row MATCH (n:Person {name: row[0]}) "
               "WITH n.name AS name, row[1] AS colour WHERE colour = 'red' RETURN name, colour",
               {{"Remy", "red"}});
}

// A record the graph answers with nothing drops out of the join, so a file naming only
// absent nodes reports no row.
TEST_F(LoadCSVMatchTest, reportsNoRowWhenNoRecordMatches) {
    writeFile("absent.csv", "Nobody,green\n");

    expectRows("LOAD CSV 'absent.csv' AS row MATCH (n:Person {name: row[0]}) RETURN n.name", {});
}
