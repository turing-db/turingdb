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

// A label or an edge type the graph does not carry is a pattern that matches nothing, not
// an ill-formed query. The predicate form already answers false rather than failing, and
// the scans are built to emit no rows for a name they cannot resolve.
class AbsentLabelAndTypeTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(AbsentLabelAndTypeTest, matchesNoNodeOfAnAbsentLabel) {
    expectRows("MATCH (n:NoSuchLabel) RETURN n.name", {});
}

TEST_F(AbsentLabelAndTypeTest, countsNoNodeOfAnAbsentLabel) {
    expectRows("MATCH (n:NoSuchLabel) RETURN count(n)", {{"0"}});
}

TEST_F(AbsentLabelAndTypeTest, matchesNoNodeCarryingAnAbsentLabelBesideAKnownOne) {
    expectRows("MATCH (n:Person:NoSuchLabel) RETURN n.name", {});
}

TEST_F(AbsentLabelAndTypeTest, matchesNoEdgeOfAnAbsentType) {
    expectRows("MATCH (n:Person)-[:NOSUCHTYPE]->(m) RETURN m.name", {});
}

TEST_F(AbsentLabelAndTypeTest, matchesNoEdgeOfAnAbsentTypeOverTheWholeGraph) {
    expectRows("MATCH ()-[e:NOSUCHTYPE]->() RETURN count(e)", {{"0"}});
}

// The whole point of the clause: a pattern that matches nothing leaves every input row in
// place with the pattern's variables null.
TEST_F(AbsentLabelAndTypeTest, padsEveryRowOfAnOptionalMatchOnAnAbsentType) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:NOSUCHTYPE]->(m) RETURN count(*)", {{"8"}});
}

TEST_F(AbsentLabelAndTypeTest, bindsNoVariableOfAnOptionalMatchOnAnAbsentType) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:NOSUCHTYPE]->(m) RETURN count(m)", {{"0"}});
}

TEST_F(AbsentLabelAndTypeTest, testsAnAbsentLabelAsAPredicate) {
    expectRows("MATCH (n) WHERE n:NoSuchLabel RETURN count(n)", {{"0"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
