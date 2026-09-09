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

// labels() answers a list of strings, so it composes with the operations a list column
// takes. Bio is the simpledb node carrying Interest and nothing else, which keeps the
// expected list one element long whatever order a label set reports its labels in.
class LabelsListTest : public TuringTest {
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

TEST_F(LabelsListTest, readsTheLabelsOfANodeAsAList) {
    expectRows("MATCH (n {name: 'Bio'}) RETURN labels(n) AS ls", {{"Interest"}});
}

TEST_F(LabelsListTest, appendsToTheLabelsOfANode) {
    expectRows("MATCH (n {name: 'Bio'}) RETURN labels(n) + ['x'] AS ls", {{"Interest, x"}});
}

TEST_F(LabelsListTest, prependsToTheLabelsOfANode) {
    expectRows("MATCH (n {name: 'Bio'}) RETURN ['x'] + labels(n) AS ls", {{"x, Interest"}});
}

TEST_F(LabelsListTest, concatenatesTheLabelsOfANodeWithTheEmptyList) {
    expectRows("MATCH (n {name: 'Bio'}) RETURN labels(n) + [] AS ls", {{"Interest"}});
}

TEST_F(LabelsListTest, concatenatesTheLabelsOfTwoNodes) {
    expectRows("MATCH (a {name: 'Bio'}), (b {name: 'Cooking'}) RETURN labels(a) + labels(b) AS ls",
               {{"Interest, Interest"}});
}

TEST_F(LabelsListTest, unwindsTheLabelsOfANodeAppendedTo) {
    expectRows("MATCH (n {name: 'Bio'}) UNWIND labels(n) + ['x'] AS label RETURN label",
               {{"Interest"}, {"x"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
