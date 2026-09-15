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

// coalesce answers the first argument that is not null. A literal is never null, so it
// settles the answer wherever it stands and every argument behind it is unreachable, and
// the answer is one per matched row whatever the arguments are.
class CoalesceLiteralArgumentTest : public TuringTest {
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

TEST_F(CoalesceLiteralArgumentTest, answersTheLiteralThatStandsBeforeAnother) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.dob, 'a', 'b')",
               {{"Remy", "18/01"},
                {"Adam", "18/08"},
                {"Maxime", "24/07"},
                {"Luc", "28/05"},
                {"Martina", "a"},
                {"Suhas", "a"},
                {"Cyrus", "a"},
                {"Doruk", "a"}});
}

TEST_F(CoalesceLiteralArgumentTest, answersALeadingLiteral) {
    expectRows("MATCH (n:Person) RETURN coalesce('a', n.dob)",
               {{"a"}, {"a"}, {"a"}, {"a"}, {"a"}, {"a"}, {"a"}, {"a"}});
}

TEST_F(CoalesceLiteralArgumentTest, answersALiteralOnEveryMatchedRow) {
    expectRows("MATCH (n:Person) RETURN coalesce(1, 2)",
               {{"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}});
}

TEST_F(CoalesceLiteralArgumentTest, answersTheLiteralBehindAnAbsentProperty) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.dob, 'a')",
               {{"Remy", "18/01"},
                {"Adam", "18/08"},
                {"Maxime", "24/07"},
                {"Luc", "28/05"},
                {"Martina", "a"},
                {"Suhas", "a"},
                {"Cyrus", "a"},
                {"Doruk", "a"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
