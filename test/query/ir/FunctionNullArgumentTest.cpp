#include <gtest/gtest.h>

#include <algorithm>
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

class FunctionNullArgumentTest : public TuringTest {
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

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(FunctionNullArgumentTest, ScalarFunctionOfTheNullLiteralIsNull) {
    expectRows("RETURN size(null)", {{"null"}});
    expectRows("RETURN toInteger(null)", {{"null"}});
    expectRows("RETURN toString(null)", {{"null"}});
    expectRows("RETURN head(null)", {{"null"}});
    expectRows("RETURN tail(null)", {{"null"}});
    expectRows("RETURN type(null)", {{"null"}});
    expectRows("RETURN id(null)", {{"null"}});
}

TEST_F(FunctionNullArgumentTest, ScalarFunctionOfANullVariableIsNull) {
    expectRows("WITH null AS s RETURN size(s)", {{"null"}});
}

TEST_F(FunctionNullArgumentTest, AnyNullArgumentMakesTheCallNull) {
    expectRows("RETURN range(1, null)", {{"null"}});
}

TEST_F(FunctionNullArgumentTest, ScalarFunctionOfANullPerRow) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN n.name, size(null)", {{"Remy", "null"}});
}

TEST_F(FunctionNullArgumentTest, AggregatesOverNullKeepTheirOwnAnswer) {
    expectRows("RETURN count(null)", {{"0"}});
    expectRows("RETURN collect(null)", {{"[]"}});
}
