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

// Equal types compare by value and different types compare false: a number against a
// number reads the row, and an entity against a string folds to false without one
class EqualityOperatorTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

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

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"equality"};
};

TEST_F(EqualityOperatorTest, anIntegerEqualsTheDoubleOfItsValue) {
    expectRows("RETURN 32 = 32.0 AS b", {{"true"}});
    expectRows("RETURN 32 = 32.5 AS b", {{"false"}});
    expectRows("RETURN 32 <> 32.5 AS b", {{"true"}});
}

TEST_F(EqualityOperatorTest, anEntityIsNotAString) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n = 'Remy' AS b", {{"false"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n <> 'Remy' AS b", {{"true"}});
    expectRows("MATCH (n:Person) WHERE n = 'Remy' RETURN count(n)", {{"0"}});
}

TEST_F(EqualityOperatorTest, aNodeIsNotAnEdge) {
    expectRows("MATCH (n:Person {name: 'Remy'})-[e]->(m) RETURN DISTINCT n = e AS b", {{"false"}});
}

TEST_F(EqualityOperatorTest, aNodeStillComparesAgainstItsId) {
    expectRows("MATCH (n:Person) WHERE n = 0 RETURN n.name", {{"Remy"}});
}

TEST_F(EqualityOperatorTest, aPropertyComparesAgainstADoubleByValue) {
    expectRows("MATCH (n:Person) WHERE n.age = 1.5 RETURN count(n)", {{"0"}});
}

TEST_F(EqualityOperatorTest, anOptionalEntityThatIsAbsentComparesAsNull) {
    expectRows("OPTIONAL MATCH (n:Person {name: 'Nobody'}) RETURN n = 'x' AS b", {{"null"}});
    expectRows("OPTIONAL MATCH (n:Person {name: 'Remy'}) RETURN n = 'x' AS b", {{"false"}});
}
