#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

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

// `+` joins a number to a string as the text Cypher writes that number with, from either
// side. `||` does not: it concatenates two strings or two lists and nothing else.
class NumberConcatenationTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        EXPECT_FALSE(status.isOk()) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(NumberConcatenationTest, joinsAnIntegerToAString) {
    expectRows("RETURN 1 + ' apples' AS said", {{"1 apples"}});
}

TEST_F(NumberConcatenationTest, joinsAStringToAnInteger) {
    expectRows("RETURN 'total: ' + 42 AS said", {{"total: 42"}});
}

TEST_F(NumberConcatenationTest, joinsADoubleToAString) {
    expectRows("RETURN 1.5 + ' kilos' AS said", {{"1.5 kilos"}});
}

TEST_F(NumberConcatenationTest, keepsThePointOfAWholeDouble) {
    expectRows("RETURN 2.0 + ' kilos' AS said", {{"2.0 kilos"}});
}

TEST_F(NumberConcatenationTest, addsBeforeItJoins) {
    expectRows("RETURN 1 + 2 + ' apples' AS said", {{"3 apples"}});
}

TEST_F(NumberConcatenationTest, joinsANumberProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age + ' years' AS said", {{"32 years"}});
}

TEST_F(NumberConcatenationTest, answersNullForANullNumber) {
    expectRows("RETURN null + ' apples' AS said", {{"null"}});
}

TEST_F(NumberConcatenationTest, answersNullForAMissingProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.height + ' metres' AS said", {{"null"}});
}

TEST_F(NumberConcatenationTest, joinsNoNumberWithTheConcatenationOperator) {
    expectError("RETURN 1 || ' apples' AS said");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
