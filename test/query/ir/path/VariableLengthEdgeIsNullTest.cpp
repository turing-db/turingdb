#include <gtest/gtest.h>

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

class VariableLengthEdgeIsNullTest : public TuringTest {
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

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(VariableLengthEdgeIsNullTest, returnsIsNull) {
    expectRows("MATCH ()-[e*1..2]->() RETURN e IS NULL AS missing, count(*)", {{"false", "30"}});
}

TEST_F(VariableLengthEdgeIsNullTest, filtersOnIsNull) {
    expectRows("MATCH ()-[e*1..2]->() WHERE e IS NULL RETURN count(*)", {{"0"}});
}

TEST_F(VariableLengthEdgeIsNullTest, filtersOnIsNotNull) {
    expectRows("MATCH ()-[e*1..2]->() WHERE e IS NOT NULL RETURN count(*)", {{"30"}});
}

TEST_F(VariableLengthEdgeIsNullTest, keepsAZeroHopWalk) {
    expectRows("MATCH ()-[e*0..1]->() WHERE e IS NOT NULL RETURN count(*)", {{"36"}});
}

TEST_F(VariableLengthEdgeIsNullTest, readsAnOptionalWalk) {
    expectRows("MATCH (n) OPTIONAL MATCH (n)-[e*1..2]->() RETURN e IS NULL AS missing, count(*) ORDER BY missing",
               {{"false", "30"}, {"true", "9"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
