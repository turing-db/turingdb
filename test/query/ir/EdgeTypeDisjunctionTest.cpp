#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A relationship pattern naming several types matches an edge carrying any one of them.
// SimpleGraph holds 18 edges over exactly two types - 15 INTERESTED_IN and 3 KNOWS_WELL -
// so naming both of them is the same as naming none.
class EdgeTypeDisjunctionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void collectRows(std::string_view query, Rows& rows) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        sink.sortedRows(rows);
    }

    void expectSameRows(std::string_view query, std::string_view reference) {
        Rows actual;
        collectRows(query, actual);

        Rows expected;
        collectRows(reference, expected);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query
                                    << "\nreference: " << reference
                                    << "\nactual:\n" << actualText;
    }

    // An edge carries exactly one type, so a write naming several has no type to give it
    void expectRejectedAsMultiType(std::string_view query) {
        NullSink sink;
        const QueryStatus status = runQuery(query, &sink);

        EXPECT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find("An edge cannot have more than one edge type"), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRowCount(std::string_view query, size_t rowCount) {
        Rows rows;
        collectRows(query, rows);

        std::string actualText;
        describeRows(rows, actualText);

        EXPECT_EQ(rows.size(), rowCount) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(EdgeTypeDisjunctionTest, NamingEveryTypeMatchesTheUnfilteredHop) {
    expectRowCount("MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a.name, b.name", 18);
    expectSameRows("MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a.name, b.name",
                   "MATCH (a)-[e]->(b) RETURN a.name, b.name");
}

TEST_F(EdgeTypeDisjunctionTest, DisjunctionIsTheUnionOfItsTypes) {
    expectRowCount("MATCH (a)-[:KNOWS_WELL]->(b) RETURN a.name, b.name", 3);
    expectRowCount("MATCH (a)-[:INTERESTED_IN]->(b) RETURN a.name, b.name", 15);

    Rows knowsWell;
    collectRows("MATCH (a)-[:KNOWS_WELL]->(b) RETURN a.name, b.name", knowsWell);

    Rows interestedIn;
    collectRows("MATCH (a)-[:INTERESTED_IN]->(b) RETURN a.name, b.name", interestedIn);

    Rows expected = knowsWell;
    expected.insert(expected.end(), interestedIn.begin(), interestedIn.end());
    std::sort(expected.begin(), expected.end());

    Rows actual;
    collectRows("MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a.name, b.name", actual);

    EXPECT_EQ(actual, expected);
}

TEST_F(EdgeTypeDisjunctionTest, TypeOrderDoesNotMatter) {
    expectSameRows("MATCH (a)-[:INTERESTED_IN|KNOWS_WELL]->(b) RETURN a.name, b.name",
                   "MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a.name, b.name");
}

// openCypher allows the colon on every type as well as on the first one only
TEST_F(EdgeTypeDisjunctionTest, ColonBeforeEachTypeIsTheSameSpelling) {
    expectSameRows("MATCH (a)-[:KNOWS_WELL|:INTERESTED_IN]->(b) RETURN a.name, b.name",
                   "MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a.name, b.name");
}

TEST_F(EdgeTypeDisjunctionTest, ATypeNoEdgeCarriesDropsOutOfTheDisjunction) {
    expectSameRows("MATCH (a)-[:KNOWS_WELL|ROBOTS]->(b) RETURN a.name, b.name",
                   "MATCH (a)-[:KNOWS_WELL]->(b) RETURN a.name, b.name");
}

TEST_F(EdgeTypeDisjunctionTest, EveryTypeAbsentFromTheSchemaMatchesNothing) {
    expectRowCount("MATCH (a)-[:ROBOTS|ALIENS]->(b) RETURN a.name, b.name", 0);
}

TEST_F(EdgeTypeDisjunctionTest, RepeatingATypeChangesNothing) {
    expectSameRows("MATCH (a)-[:KNOWS_WELL|KNOWS_WELL]->(b) RETURN a.name, b.name",
                   "MATCH (a)-[:KNOWS_WELL]->(b) RETURN a.name, b.name");
}

TEST_F(EdgeTypeDisjunctionTest, BindingTheEdgeKeepsTheSameRows) {
    expectSameRows("MATCH (a)-[e:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a.name, b.name",
                   "MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a.name, b.name");
}

TEST_F(EdgeTypeDisjunctionTest, TheEdgeTypeIsReadableOffTheBoundEdge) {
    expectRowCount("MATCH (a)-[e:KNOWS_WELL|INTERESTED_IN]->(b) RETURN type(e)", 18);
}

TEST_F(EdgeTypeDisjunctionTest, InEdgeDirection) {
    expectRowCount("MATCH (a)<-[:KNOWS_WELL|INTERESTED_IN]-(b) RETURN a.name, b.name", 18);
    expectSameRows("MATCH (a)<-[:KNOWS_WELL|INTERESTED_IN]-(b) RETURN a.name, b.name",
                   "MATCH (a)<-[e]-(b) RETURN a.name, b.name");
}

// Undirected walks every edge from both of its endpoints, so each of the 18 gives two rows
TEST_F(EdgeTypeDisjunctionTest, Undirected) {
    expectRowCount("MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]-(b) RETURN a.name, b.name", 36);
    expectSameRows("MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]-(b) RETURN a.name, b.name",
                   "MATCH (a)-[e]-(b) RETURN a.name, b.name");
}

TEST_F(EdgeTypeDisjunctionTest, SecondHopOfATwoHopPattern) {
    expectSameRows("MATCH (a)-[:KNOWS_WELL]->(b)-[:KNOWS_WELL|INTERESTED_IN]->(c) RETURN a.name, c.name",
                   "MATCH (a)-[:KNOWS_WELL]->(b)-[e]->(c) RETURN a.name, c.name");
}

TEST_F(EdgeTypeDisjunctionTest, NarrowsAgainstAnInlinePropertyConstraint) {
    expectSameRows("MATCH (a)-[:KNOWS_WELL|INTERESTED_IN {proficiency: 'expert'}]->(b) RETURN a.name, b.name",
                   "MATCH (a)-[e {proficiency: 'expert'}]->(b) RETURN a.name, b.name");
}

TEST_F(EdgeTypeDisjunctionTest, CreatePatternStillRejectsMoreThanOneType) {
    expectRejectedAsMultiType("CREATE (a:Person)-[:KNOWS_WELL|INTERESTED_IN]->(b:Person)");
}

// MERGE reaches the same guard as CREATE: it writes the edge when it finds none, so it
// needs the one type to give it.
TEST_F(EdgeTypeDisjunctionTest, MergePatternStillRejectsMoreThanOneType) {
    expectRejectedAsMultiType("MERGE (a:Person)-[:KNOWS_WELL|INTERESTED_IN]->(b:Person)");
}

// The single-type MERGE the disjunction is built from gets past the type guard - it
// fails later, for needing a change to write in - so the rejection above is about the
// several types, not about MERGE carrying a type at all.
TEST_F(EdgeTypeDisjunctionTest, MergePatternWithOneTypeClearsTheTypeGuard) {
    NullSink sink;
    const QueryStatus status = runQuery("MERGE (a:Person)-[:KNOWS_WELL]->(b:Person)", &sink);

    EXPECT_EQ(status.getError().find("An edge cannot have more than one edge type"), std::string::npos)
        << "error: " << status.getError();
}
