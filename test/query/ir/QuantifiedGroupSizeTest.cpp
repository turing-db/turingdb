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

class QuantifiedGroupSizeTest : public TuringTest {
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

    void expectError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
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

TEST_F(QuantifiedGroupSizeTest, sizesASourceGroup) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN size(a) AS s, count(*) ORDER BY s",
               {{"1", "18"}, {"2", "12"}});
}

TEST_F(QuantifiedGroupSizeTest, sizesATargetGroup) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN size(b) AS s, count(*) ORDER BY s",
               {{"1", "18"}, {"2", "12"}});
}

TEST_F(QuantifiedGroupSizeTest, sizesAGroupAsItsEdges) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE size(a) = size(r) RETURN count(*)", {{"30"}});
}

TEST_F(QuantifiedGroupSizeTest, sizesAGroupPastAWith) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WITH a RETURN size(a) AS s, count(*) ORDER BY s",
               {{"1", "18"}, {"2", "12"}});
}

TEST_F(QuantifiedGroupSizeTest, rejectsSizeOfANode) {
    expectError("MATCH (n) RETURN size(n)", "its argument is a single entity");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
