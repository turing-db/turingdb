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

// An indexed list element evaluates to the element itself, so it feeds an operator like
// any other value of its type, and an alias carries it on unchanged.
class ListElementOperandTest : public TuringTest {
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

TEST_F(ListElementOperandTest, addsToAnIndexedElement) {
    expectRows("RETURN [1,2,3][1] + 1", {{"3"}});
}

TEST_F(ListElementOperandTest, multipliesAnIndexedElement) {
    expectRows("RETURN [1,2,3][1] * 2", {{"4"}});
}

TEST_F(ListElementOperandTest, ordersAPropertyAgainstAnIndexedElement) {
    expectRows("MATCH (n:Person) WHERE [30,40][0] < n.age RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(ListElementOperandTest, carriesAnIndexedElementThroughAnAlias) {
    expectRows("WITH [1,2,3][1] AS x RETURN x + 1", {{"3"}});
}

TEST_F(ListElementOperandTest, comparesAnIndexedElementForEquality) {
    expectRows("MATCH (n:Person) WHERE [n.name][0] = 'Remy' RETURN n.name", {{"Remy"}});
}

TEST_F(ListElementOperandTest, dividesAnIndexedIntegerElement) {
    expectRows("RETURN [7,2][0] / 2", {{"3"}});
}

TEST_F(ListElementOperandTest, dividesAnIndexedIntegerElementThroughAnAlias) {
    expectRows("WITH [7,2][0] AS x RETURN x / 2", {{"3"}});
}

TEST_F(ListElementOperandTest, dividesAnIndexedPropertyElement) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN [n.age][0] / 5", {{"6"}});
}

TEST_F(ListElementOperandTest, dividesAnIndexedDoubleElement) {
    expectRows("RETURN [7.0,2.0][0] / 2", {{"3.5"}});
}

TEST_F(ListElementOperandTest, dividesAnIndexedCellOfAMixedList) {
    expectRows("RETURN [7,2.0][0] / 2", {{"3.5"}});
}

TEST_F(ListElementOperandTest, concatenatesAnIndexedStringElement) {
    expectRows("RETURN ['ab','cd'][0] + 'ef'", {{"abef"}});
}

TEST_F(ListElementOperandTest, readsAnIndexedStringElement) {
    expectRows("RETURN ['ab','cd'][1]", {{"cd"}});
}

TEST_F(ListElementOperandTest, readsAnIndexedBooleanElement) {
    expectRows("RETURN [true,false][0]", {{"true"}});
}

TEST_F(ListElementOperandTest, readsAnIndexPastTheEndAsNull) {
    expectRows("RETURN [1,2,3][7]", {{"null"}});
}

TEST_F(ListElementOperandTest, readsAnIndexedCellOfAMixedList) {
    expectRows("RETURN [1,'ab'][1]", {{"ab"}});
}

TEST_F(ListElementOperandTest, readsANullCellOfAMixedList) {
    expectRows("RETURN [1,null,3][1]", {{"null"}});
}

TEST_F(ListElementOperandTest, readsAnIndexPastTheEndOfAMixedListAsNull) {
    expectRows("RETURN [1,'ab'][7]", {{"null"}});
}

TEST_F(ListElementOperandTest, addsToAnIndexedCellOfAMixedList) {
    expectRows("RETURN [1,'ab'][0] + 1", {{"2"}});
}

TEST_F(ListElementOperandTest, comparesAnIndexedCellOfAMixedListForEquality) {
    expectRows("MATCH (n:Person) WHERE [1,'Remy'][1] = n.name RETURN n.name", {{"Remy"}});
}

TEST_F(ListElementOperandTest, ordersAPropertyAgainstAnIndexedCellOfAMixedList) {
    expectRows("MATCH (n:Person) WHERE [30,'x'][0] < n.age RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(ListElementOperandTest, indexesTheListAnUnwindBinds) {
    expectRows("UNWIND [[1,2],[3,4]] AS xs RETURN xs[0]", {{"1"}, {"3"}});
}

TEST_F(ListElementOperandTest, computesOverTheListAnUnwindBinds) {
    expectRows("UNWIND [[1,2],[3,4]] AS xs RETURN xs[0] + 1", {{"2"}, {"4"}});
}

TEST_F(ListElementOperandTest, indexesByThePositionAnUnwindBinds) {
    expectRows("UNWIND [1,2,3] AS i RETURN [10,20,30][i - 1]", {{"10"}, {"20"}, {"30"}});
}

TEST_F(ListElementOperandTest, unwindsAnIndexedList) {
    expectRows("UNWIND [[1,2],[3,4]][0] AS x RETURN x", {{"1"}, {"2"}});
    expectRows("UNWIND [[1,2],[3,4]][1] AS x RETURN x", {{"3"}, {"4"}});
}

TEST_F(ListElementOperandTest, unwindsAnIndexPastTheEndToNoRow) {
    expectRows("UNWIND [[1,2],[3,4]][7] AS x RETURN x", {});
}

TEST_F(ListElementOperandTest, unwindsANullCellToNoRow) {
    expectRows("UNWIND [null,[1,2]][0] AS x RETURN x", {});
}

TEST_F(ListElementOperandTest, unwindsAnIndexedListOutOfAMixedList) {
    expectRows("UNWIND [[1,2],'ab'][0] AS x RETURN x", {{"1"}, {"2"}});
}

TEST_F(ListElementOperandTest, unwindsAnIndexedScalarToItself) {
    expectRows("UNWIND [[1,2],'ab'][1] AS x RETURN x", {{"ab"}});
}

TEST_F(ListElementOperandTest, unwindsAnIndexedListForEveryMatchedRow) {
    expectRows("MATCH (n:Person) WHERE n.age = 32 UNWIND [[1,2],[3,4]][0] AS x RETURN x",
               {{"1"}, {"1"}, {"2"}, {"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
