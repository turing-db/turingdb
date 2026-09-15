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

// A property's type is carried per node rather than per label, so an equality between two
// of them is answered row by row. Values of different types compare as null, which matches
// no row rather than raising.
class CrossTypeComparisonTest : public TuringTest {
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

TEST_F(CrossTypeComparisonTest, comparesAnIntegerPropertyToAStringProperty) {
    expectRows("MATCH (a:Person) WHERE a.age = a.name RETURN count(*)", {{"0"}});
}

TEST_F(CrossTypeComparisonTest, comparesTwoPropertiesAcrossTwoPatterns) {
    expectRows("MATCH (a:Person), (b:Person) WHERE a.age = b.name RETURN count(*)", {{"0"}});
}

TEST_F(CrossTypeComparisonTest, comparesAPropertyToALiteralOfAnotherType) {
    expectRows("MATCH (a:Person) WHERE a.age = 'Remy' RETURN count(*)", {{"0"}});
}

TEST_F(CrossTypeComparisonTest, matchesTheRowsWhoseTypesAgree) {
    expectRows("MATCH (a:Person) WHERE a.age = 32 RETURN count(*)", {{"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
