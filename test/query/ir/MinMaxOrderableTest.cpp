#include <gtest/gtest.h>

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

// min and max reduce a group to one of the values it holds, which asks only that the
// values be ordered against each other. Strings and booleans are ordered by the language,
// so they reduce as numbers do rather than being turned away as invalid arguments.
class MinMaxOrderableTest : public TuringTest {
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

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MinMaxOrderableTest, reducesTheLeastAndGreatestName) {
    expectRowsInOrder("MATCH (p:Person) RETURN min(p.name), max(p.name)", {{"Adam", "Suhas"}});
}

// The two extremes of each person's interests, which for a group of one are both that one
// value.
TEST_F(MinMaxOrderableTest, reducesTheLeastAndGreatestNameOfEachGroup) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                      "RETURN p.name, min(i.name), max(i.name) ORDER BY p.name",
                      {{"Adam", "Bio", "Cooking"},
                       {"Cyrus", "Gym", "Travel"},
                       {"Doruk", "Gym", "Gym"},
                       {"Luc", "Animals", "Computers"},
                       {"Martina", "Cooking", "Cooking"},
                       {"Maxime", "Bio", "Padel"},
                       {"Remy", "Computers", "Ghosts"},
                       {"Suhas", "Gym", "JiuJitsu"}});
}

// The interests that carry the property hold both booleans, and false orders below true.
// The ones carrying no isReal at all reduce to nothing, as a null does for every other
// aggregate.
TEST_F(MinMaxOrderableTest, reducesTheLeastAndGreatestBoolean) {
    expectRowsInOrder("MATCH (i:Interest) RETURN min(i.isReal), max(i.isReal)", {{"false", "true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
