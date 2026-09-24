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

// toString() writes a value as Cypher prints it: a double keeps its point, a boolean is a
// word, and a string is itself.
class ToStringFunctionTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ToStringFunctionTest, writesAnInteger) {
    expectRows("RETURN toString(1) AS s", {{"1"}});
}

TEST_F(ToStringFunctionTest, writesANegativeInteger) {
    expectRows("RETURN toString(-12) AS s", {{"-12"}});
}

TEST_F(ToStringFunctionTest, writesADouble) {
    expectRows("RETURN toString(1.5) AS s", {{"1.5"}});
}

TEST_F(ToStringFunctionTest, writesAWholeDoubleWithItsPoint) {
    expectRows("RETURN toString(1.0) AS s", {{"1.0"}});
}

TEST_F(ToStringFunctionTest, writesABoolean) {
    expectRows("RETURN toString(true) AS t, toString(false) AS f", {{"true", "false"}});
}

TEST_F(ToStringFunctionTest, writesAStringAsItself) {
    expectRows("RETURN toString('a') AS s", {{"a"}});
}

TEST_F(ToStringFunctionTest, writesAPropertyOfEveryRow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN toString(n.age) AS age", {{"32"}});
}

TEST_F(ToStringFunctionTest, writesTheElementsOfAList) {
    expectRows("RETURN [i IN range(0, 3) | toString(i)] AS written", {{"[0, 1, 2, 3]"}});
}

TEST_F(ToStringFunctionTest, concatenatesWhatItWrote) {
    expectRows("RETURN toString(1) + ': ' + toString(2) AS s", {{"1: 2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
