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

// A label or an edge type the graph never assigned matches nothing: the pattern it
// constrains is empty, and the query around it runs to completion.
class AbsentSchemaNameTest : public TuringTest {
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

    void expectNoRows(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_TRUE(sink.rows().empty()) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(AbsentSchemaNameTest, AbsentLabelScanMatchesNothing) {
    expectNoRows("MATCH (n:NoSuchLabel) RETURN n.name");
}

// The labels of a node pattern are a conjunction, so one absent name empties the whole
// pattern instead of weakening it to the names the graph does know
TEST_F(AbsentSchemaNameTest, AbsentLabelBesideAKnownOneMatchesNothing) {
    expectNoRows("MATCH (n:Person:NoSuchLabel) RETURN n.name");
}

TEST_F(AbsentSchemaNameTest, AbsentLabelOnTheTargetOfAHopMatchesNothing) {
    expectNoRows("MATCH (n:Person)-[:KNOWS_WELL]->(m:NoSuchLabel) RETURN m.name");
}

TEST_F(AbsentSchemaNameTest, AHopOutOfAnAbsentLabelMatchesNothing) {
    expectNoRows("MATCH (n:NoSuchLabel)-[:KNOWS_WELL]->(m) RETURN m.name");
}

TEST_F(AbsentSchemaNameTest, AbsentEdgeTypeMatchesNothing) {
    expectNoRows("MATCH (n:Person)-[:NOSUCHTYPE]->(m) RETURN m.name");
}

TEST_F(AbsentSchemaNameTest, AbsentEdgeTypeOnAnIncomingHopMatchesNothing) {
    expectNoRows("MATCH (n:Person)<-[:NOSUCHTYPE]-(m) RETURN m.name");
}

// The keyless count still emits its one group over the empty scan
TEST_F(AbsentSchemaNameTest, CountOverAnAbsentLabelIsZero) {
    expectCounts("MATCH (n:NoSuchLabel) RETURN count(n)", {0});
}

// The same test written as a predicate, which already answered false per row
TEST_F(AbsentSchemaNameTest, CountOverAnAbsentLabelPredicateIsZero) {
    expectCounts("MATCH (n) WHERE n:NoSuchLabel RETURN count(n)", {0});
}

// An OPTIONAL MATCH of an absent edge type misses every row it joins onto, so all 8
// people come back null-padded
TEST_F(AbsentSchemaNameTest, OptionalMatchOfAnAbsentEdgeTypePadsEveryRow) {
    expectCounts("MATCH (n:Person) OPTIONAL MATCH (n)-[:NOSUCHTYPE]->(m) RETURN count(*)", {8});
}
