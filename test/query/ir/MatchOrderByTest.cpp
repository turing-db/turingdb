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

// The query test suite's fail-reads-order-by-0 case on the v3 engine. An ORDER BY written on
// the MATCH orders the rows the match produced, which the RETURN then reads in that order.
class MatchOrderByTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRowsInOrder(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// fail-reads-order-by-0: the eighteen nodes of simpledb by name, Adam (1) first and Travel
// (14) last, where the match alone would answer them by id
TEST_F(MatchOrderByTest, ordersTheMatchedRowsByTheKeyTheMatchNames) {
    const std::vector<StringRowSink::Row> expected {{"1"},
                                                    {"10"},
                                                    {"4"},
                                                    {"2"},
                                                    {"5"},
                                                    {"15"},
                                                    {"17"},
                                                    {"3"},
                                                    {"6"},
                                                    {"13"},
                                                    {"16"},
                                                    {"9"},
                                                    {"11"},
                                                    {"8"},
                                                    {"7"},
                                                    {"0"},
                                                    {"12"},
                                                    {"14"}};

    expectRowsInOrder("MATCH (n) ORDER BY n.name RETURN n", expected);
}

TEST_F(MatchOrderByTest, ordersTheMatchedRowsDownwards) {
    const std::vector<StringRowSink::Row> expected {{"14"},
                                                    {"12"},
                                                    {"0"},
                                                    {"7"},
                                                    {"8"},
                                                    {"11"},
                                                    {"9"},
                                                    {"16"},
                                                    {"13"},
                                                    {"6"},
                                                    {"3"},
                                                    {"17"},
                                                    {"15"},
                                                    {"5"},
                                                    {"2"},
                                                    {"4"},
                                                    {"10"},
                                                    {"1"}};

    expectRowsInOrder("MATCH (n) ORDER BY n.name DESC RETURN n", expected);
}

// The order is over the rows the match kept, so the label constraint cuts the eighteen down
// to the eight Person nodes before they are ordered
TEST_F(MatchOrderByTest, ordersOnlyTheRowsTheMatchKept) {
    const std::vector<StringRowSink::Row> expected {{"1"}, {"15"}, {"17"}, {"9"}, {"11"}, {"8"}, {"0"}, {"12"}};

    expectRowsInOrder("MATCH (n:Person) ORDER BY n.name RETURN n", expected);
}

// A property the RETURN reads is read from the ordered rows, not from the ones the match
// produced before the sort
TEST_F(MatchOrderByTest, readsAProjectedPropertyFromTheOrderedRows) {
    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}};

    expectRowsInOrder("MATCH (n:Person) ORDER BY n.name RETURN n.name", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
