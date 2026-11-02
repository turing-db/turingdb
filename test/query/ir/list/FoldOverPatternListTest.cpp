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

// The variable of a variable-length relationship and a group variable of a quantified path
// are lists, which reduce, allReduce and every other list operation read as they read any
// list. KNOWS_WELL leaves Remy for Adam and Adam for Remy, both of duration 20, and Ghosts
// for Remy, of duration 200.
class FoldOverPatternListTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
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

TEST_F(FoldOverPatternListTest, countsTheHopsOfEachPath) {
    expectRows("MATCH ()-[r:KNOWS_WELL*1..2]->() RETURN reduce(s = 0, e IN r | s + 1) AS n",
               {{"1"}, {"1"}, {"1"}, {"2"}, {"2"}, {"2"}});
}

TEST_F(FoldOverPatternListTest, sumsAPropertyOfTheRelationships) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) RETURN b.name, reduce(s = 0, e IN r | s + e.duration)",
               {{"Adam", "20"}, {"Remy", "40"}});
}

TEST_F(FoldOverPatternListTest, decidesAnAllReduceOverTheRelationships) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) "
               "RETURN b.name, allReduce(s = 0, e IN r | s + e.duration, s < 30)",
               {{"Adam", "true"}, {"Remy", "false"}});
}

TEST_F(FoldOverPatternListTest, filtersOnAnAllReduceOverTheRelationships) {
    expectRows("MATCH (a)-[r:KNOWS_WELL*1..3]->(b) "
               "WHERE allReduce(s = 0, e IN r | s + e.duration, s < 100) "
               "RETURN a.name, b.name",
               {{"Remy", "Adam"}, {"Remy", "Remy"}, {"Adam", "Remy"}, {"Adam", "Adam"}});
}

TEST_F(FoldOverPatternListTest, sumsAPropertyOfAGroupRelationshipVariable) {
    expectRows("MATCH (:Person {name: 'Remy'}) ((x)-[e:KNOWS_WELL]->(y)){1,2} (b) "
               "RETURN b.name, reduce(s = 0, r IN e | s + r.duration)",
               {{"Adam", "20"}, {"Remy", "40"}});
}

TEST_F(FoldOverPatternListTest, filtersOnAnAllReduceOverAGroupRelationshipVariable) {
    expectRows("MATCH (:Person {name: 'Remy'}) ((x)-[e:KNOWS_WELL]->(y)){1,2} (b) "
               "WHERE allReduce(s = 0, r IN e | s + r.duration, s < 30) "
               "RETURN b.name",
               {{"Adam"}});
}

TEST_F(FoldOverPatternListTest, foldsAGroupNodeVariable) {
    expectRows("MATCH (:Person {name: 'Remy'}) ((x)-[e:KNOWS_WELL]->(y)){1,2} (b) "
               "RETURN b.name, reduce(s = '', n IN x | s + n.name)",
               {{"Adam", "Remy"}, {"Remy", "RemyAdam"}});
}

TEST_F(FoldOverPatternListTest, takesTheHeadLastTailAndReverseOfTheRelationships) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) "
               "RETURN b.name, startNode(head(r)).name, startNode(last(r)).name, size(tail(r)), "
               "[e IN reverse(r) | startNode(e).name]",
               {{"Adam", "Remy", "Remy", "0", "Remy"}, {"Remy", "Remy", "Adam", "1", "Adam, Remy"}});
}

TEST_F(FoldOverPatternListTest, takesTheHeadAndLastOfAGroupNodeVariable) {
    expectRows("MATCH (:Person {name: 'Remy'}) ((x)-[e:KNOWS_WELL]->(y)){1,2} (b) "
               "RETURN b.name, head(x).name, last(y).name, [n IN reverse(x) | n.name]",
               {{"Adam", "Remy", "Adam", "Remy"}, {"Remy", "Remy", "Remy", "Adam, Remy"}});
}

TEST_F(FoldOverPatternListTest, slicesTheRelationships) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) RETURN b.name, size(r[1..]), [e IN r[1..] | e.duration]",
               {{"Adam", "0", ""}, {"Remy", "1", "20"}});
}

TEST_F(FoldOverPatternListTest, concatenatesTheRelationships) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) RETURN b.name, size(r + r), size(r + head(r))",
               {{"Adam", "2", "2"}, {"Remy", "4", "3"}});
}

TEST_F(FoldOverPatternListTest, findsARelationshipInTheRelationships) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) "
               "MATCH (:Person {name: 'Adam'})-[q:KNOWS_WELL]->() WHERE q IN r RETURN b.name",
               {{"Remy"}});
}

TEST_F(FoldOverPatternListTest, findsANodeInAGroupNodeVariable) {
    expectRows("MATCH (:Person {name: 'Remy'}) ((x)-[e:KNOWS_WELL]->(y)){1,2} (b) "
               "MATCH (q:Person {name: 'Adam'}) WHERE q IN x RETURN b.name",
               {{"Remy"}});
}

TEST_F(FoldOverPatternListTest, nestsTheRelationshipsInAListLiteral) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) RETURN b.name, size([r][0])",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(FoldOverPatternListTest, collectsTheRelationshipsIntoAListOfLists) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) "
               "WITH b, collect(r) AS paths UNWIND paths AS path RETURN b.name, size(path)",
               {{"Adam", "1"}, {"Remy", "2"}});
}
