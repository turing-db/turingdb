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

// min() and max() reduce the string type() answers - one the engine owns - the same way
// they reduce a stored string property. simpledb holds the two types INTERESTED_IN and
// KNOWS_WELL, so the least and the greatest are one each.
class OwnedStringReductionTest : public TuringTest {
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

TEST_F(OwnedStringReductionTest, reducesAnEdgeTypeToItsLeast) {
    expectRows("MATCH ()-[e]->() RETURN min(type(e))", {{"INTERESTED_IN"}});
}

TEST_F(OwnedStringReductionTest, reducesAnEdgeTypeToItsGreatest) {
    expectRows("MATCH ()-[e]->() RETURN max(type(e))", {{"KNOWS_WELL"}});
}

TEST_F(OwnedStringReductionTest, reducesAnEdgeTypePerGroup) {
    expectRows("MATCH (a:Person)-[e]->(b) RETURN a.name, max(type(e))",
               {{"Remy", "KNOWS_WELL"},
                {"Adam", "KNOWS_WELL"},
                {"Maxime", "INTERESTED_IN"},
                {"Luc", "INTERESTED_IN"},
                {"Martina", "INTERESTED_IN"},
                {"Suhas", "INTERESTED_IN"},
                {"Cyrus", "INTERESTED_IN"},
                {"Doruk", "INTERESTED_IN"}});
}

TEST_F(OwnedStringReductionTest, reducesAStringProperty) {
    expectRows("MATCH (n:Person) RETURN min(n.name), max(n.name)", {{"Adam", "Suhas"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
