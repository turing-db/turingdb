#include <gtest/gtest.h>

#include <memory>
#include <span>
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

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class RowCountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

}

// The query test suite's fail-reads-match-0 case on the v3 engine. A name bound to an edge
// in one pattern and to a node in another names two things that cannot be the same value,
// so the query is turned away - while the same name kept to one kind across patterns is a
// join on that entity, and runs.
class VariableTypeConflictTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectTypeConflictRejection(std::string_view query, std::string_view reason) {
        RowCountingSink sink;
        QueryStatus status;
        runQuery(query, status, sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << "query: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowCountingSink sink;
        QueryStatus status;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.getRowCount(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;

private:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }
};

// fail-reads-match-0: 'e' is the edge of the first pattern, then the end node of the second
TEST_F(VariableTypeConflictTest, rejectsAnEdgeVariableReusedAsANode) {
    expectTypeConflictRejection("MATCH (n: Person { name: \"Luc\" })-[e:KNOWS_WELL { name: \"Whatever\" }]->(m)-[e2]->(c:Person)\n"
                                "MATCH (a)-->(b)-->(e)\n"
                                "RETURN n, e, m;",
                                "Variable 'e' is already declared with type 'EdgePattern'");
}

// The same conflict the other way round, where the node comes first
TEST_F(VariableTypeConflictTest, rejectsANodeVariableReusedAsAnEdge) {
    expectTypeConflictRejection("MATCH (n)-->(m)\nMATCH (a)-[n]->(b)\nRETURN m;",
                                "Variable 'n' is already declared with type 'NodePattern'");
}

// An UNWIND of node IDs may name a pattern node, which seeds the traversal from them, so
// only a list whose items are not node IDs conflicts with the pattern.
TEST_F(VariableTypeConflictTest, rejectsAStringUnwoundIntoANodePattern) {
    expectTypeConflictRejection("UNWIND ['a', 'b'] AS x MATCH (x) RETURN x;",
                                "Variable 'x' is already declared with type 'String'");
}

TEST_F(VariableTypeConflictTest, rejectsAHeterogeneousListUnwoundIntoANodePattern) {
    expectTypeConflictRejection("UNWIND [1, 'a'] AS x MATCH (x) RETURN x;",
                                "Variable 'x' is already declared with type 'ListItem'");
}

TEST_F(VariableTypeConflictTest, rejectsAnEmptyListUnwoundIntoANodePattern) {
    expectTypeConflictRejection("UNWIND [] AS x MATCH (x) RETURN x;",
                                "Variable 'x' is already declared with type 'ListItem'");
}

// The seed must be in scope before the pattern names it: an UNWIND naming a variable the
// pattern already bound would rebind it.
TEST_F(VariableTypeConflictTest, rejectsAnUnwindOverAPatternNode) {
    expectTypeConflictRejection("MATCH (x)-->(w) UNWIND [1, 2] AS x RETURN w;",
                                "Variable 'x' is already declared");
}

// The valid neighbour of those two: 'm' stays a node in both patterns and joins them on it.
// Luc reaches Animals and Computers; Animals is entered by Luc alone, Computers by Remy and
// Luc.
TEST_F(VariableTypeConflictTest, joinsANodeVariableSharedByTwoPatterns) {
    expectRowCount("MATCH (n:Person { name: \"Luc\" })-[e]->(m)\nMATCH (a)-->(m)\nRETURN n, a;", 3);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
