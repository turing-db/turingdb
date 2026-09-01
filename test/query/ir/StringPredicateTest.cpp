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

class StringPredicateTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRows(std::string_view query, std::vector<StringRowSink::Row> expected) {
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

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(StringPredicateTest, startsWithLiteralPrefix) {
    expectRows("MATCH (n) WHERE n.name STARTS WITH 'C' RETURN n.name",
               {{"Computers"}, {"Cooking"}, {"Cyrus"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n.name",
               {{"Adam"}, {"Animals"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'Rem' RETURN n.name",
               {{"Remy"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'Z' RETURN n.name",
               {});
}

TEST_F(StringPredicateTest, endsWithLiteralSuffix) {
    expectRows("MATCH (n) WHERE n.name ENDS WITH 'o' RETURN n.name",
               {{"Bio"}});

    expectRows("MATCH (n) WHERE n.name ENDS WITH 'm' RETURN n.name",
               {{"Adam"}, {"Gym"}});

    expectRows("MATCH (n) WHERE n.name ENDS WITH 'ties' RETURN n.name",
               {{"Eighties"}});
}

TEST_F(StringPredicateTest, containsLiteralSubstring) {
    expectRows("MATCH (n) WHERE n.name CONTAINS 'oo' RETURN n.name",
               {{"Cooking"}});

    expectRows("MATCH (n) WHERE n.name CONTAINS 'us' RETURN n.name",
               {{"Cyrus"}});

    expectRows("MATCH (n) WHERE n.name CONTAINS 'o' RETURN n.name",
               {{"Computers"}, {"Bio"}, {"Cooking"}, {"Ghosts"}, {"Doruk"}});
}

TEST_F(StringPredicateTest, nonLiteralPropertyOperands) {
    expectRows("MATCH (n:Person) WHERE n.name CONTAINS n.name RETURN n.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});

    expectRows("MATCH (a)-[e]->(b) WHERE a.name = 'Remy' AND e.name STARTS WITH a.name RETURN e.name",
               {{"Remy -> Adam"}, {"Remy -> Ghosts"}, {"Remy -> Computers"}, {"Remy -> Eighties"}});
}

TEST_F(StringPredicateTest, combinedWithBooleanPredicates) {
    expectRows("MATCH (n) WHERE n.name STARTS WITH 'C' AND n.name ENDS WITH 's' RETURN n.name",
               {{"Computers"}, {"Cyrus"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'A' OR n.name STARTS WITH 'G' RETURN n.name",
               {{"Adam"}, {"Animals"}, {"Ghosts"}, {"Gym"}});
}

TEST_F(StringPredicateTest, combinedWithPropertyPredicates) {
    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'M' AND n.isFrench = true RETURN n.name",
               {{"Maxime"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'A' AND n.age = 32 RETURN n.name",
               {{"Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
