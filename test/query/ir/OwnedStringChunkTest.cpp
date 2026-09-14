#include <gtest/gtest.h>

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

// type() is the one function whose rows own the characters they hold, where a string
// property column borrows them from the graph. That makes its chunk a nullable of owned
// strings, an element type every step carrying a chunk on has to know: a limit, a skip, a
// grouping key, a cross product.
class OwnedStringChunkTest : public TuringTest {
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

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows().size(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// simpledb holds 18 edges
TEST_F(OwnedStringChunkTest, limitsAColumnOfEdgeTypes) {
    expectRowCount("MATCH ()-[e]->() RETURN type(e) LIMIT 3", 3);
}

TEST_F(OwnedStringChunkTest, skipsAColumnOfEdgeTypes) {
    expectRowCount("MATCH ()-[e]->() RETURN type(e) SKIP 2", 16);
}

TEST_F(OwnedStringChunkTest, dedupsAColumnOfEdgeTypes) {
    expectRows("MATCH ()-[e]->() RETURN DISTINCT type(e)",
               {{"KNOWS_WELL"}, {"INTERESTED_IN"}});
}

// Remy's edge type crossed with the ten interests: the one row the WITH published is
// repeated once per interest, carrying its string along
TEST_F(OwnedStringChunkTest, carriesAColumnOfEdgeTypesAcrossACrossProduct) {
    expectRows("MATCH (p:Person {name: 'Remy'})-[e:KNOWS_WELL]->() WITH type(e) AS kind "
               "MATCH (i:Interest) RETURN kind, i.name",
               {{"KNOWS_WELL", "Animals"},
                {"KNOWS_WELL", "Bio"},
                {"KNOWS_WELL", "Computers"},
                {"KNOWS_WELL", "Cooking"},
                {"KNOWS_WELL", "Eighties"},
                {"KNOWS_WELL", "Ghosts"},
                {"KNOWS_WELL", "Gym"},
                {"KNOWS_WELL", "JiuJitsu"},
                {"KNOWS_WELL", "Padel"},
                {"KNOWS_WELL", "Travel"}});
}

// An edge the pattern missed is null, so the grouping key it holds is one too: the two
// edges Remy and Adam walk form the named group, the six padded rows the null one
TEST_F(OwnedStringChunkTest, groupsOnTheEdgeTypeOfAnOptionalMatch) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->(f) "
               "RETURN type(e), count(*)",
               {{"KNOWS_WELL", "2"}, {"null", "6"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
