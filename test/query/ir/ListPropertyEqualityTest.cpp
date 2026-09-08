#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// Comparing a stored list against a list the query spells out: the pattern form
// MATCH (n {tags: [...]}) and the WHERE form, which lower to the same equality. Lists are
// equal element-wise and length-wise, so a prefix, a reordering and a differently-typed
// element all fail to match, and a node holding no list matches neither the value nor its
// negation.
class ListPropertyEqualityTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     &callbacks,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    void write(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
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

    void tagRemyAndAdam() {
        write("MATCH (n {name: 'Remy'}) SET n.tags = [1, 2]");
        write("MATCH (n {name: 'Adam'}) SET n.tags = [3]");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(ListPropertyEqualityTest, matchesAStoredListByPattern) {
    tagRemyAndAdam();
    expectRows("MATCH (n {tags: [1, 2]}) RETURN n.name", {{"Remy"}});
}

TEST_F(ListPropertyEqualityTest, matchesAStoredListInWhere) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.tags = [1, 2] RETURN n.name", {{"Remy"}});
}

TEST_F(ListPropertyEqualityTest, matchesTheOtherStoredList) {
    tagRemyAndAdam();
    expectRows("MATCH (n {tags: [3]}) RETURN n.name", {{"Adam"}});
}

TEST_F(ListPropertyEqualityTest, doesNotMatchAPrefixOfAStoredList) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.tags = [1] RETURN n.name", {});
}

TEST_F(ListPropertyEqualityTest, doesNotMatchALongerList) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.tags = [1, 2, 3] RETURN n.name", {});
}

TEST_F(ListPropertyEqualityTest, doesNotMatchAReorderedList) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.tags = [2, 1] RETURN n.name", {});
}

TEST_F(ListPropertyEqualityTest, doesNotMatchAnElementOfAnotherType) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.tags = ['1', '2'] RETURN n.name", {});
}

TEST_F(ListPropertyEqualityTest, matchesAHeterogeneousStoredList) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1, 'two', true]");
    expectRows("MATCH (n) WHERE n.tags = [1, 'two', true] RETURN n.name", {{"Remy"}});
}

TEST_F(ListPropertyEqualityTest, matchesANestedStoredList) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [[1, 2], [3]]");
    expectRows("MATCH (n) WHERE n.tags = [[1, 2], [3]] RETURN n.name", {{"Remy"}});
}

TEST_F(ListPropertyEqualityTest, doesNotMatchANestedListFlattened) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [[1, 2], [3]]");
    expectRows("MATCH (n) WHERE n.tags = [1, 2, 3] RETURN n.name", {});
}

TEST_F(ListPropertyEqualityTest, matchesAnEmptyStoredList) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = []");
    write("MATCH (n {name: 'Adam'}) SET n.tags = [3]");
    expectRows("MATCH (n) WHERE n.tags = [] RETURN n.name", {{"Remy"}});
}

TEST_F(ListPropertyEqualityTest, negatesTheComparison) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.tags <> [1, 2] RETURN n.name", {{"Adam"}});
}

// A node with no list holds null there, and null compares equal to nothing - not to the
// value, and not to its negation either.
TEST_F(ListPropertyEqualityTest, matchesNeitherSideWhereTheNodeHasNoList) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.tags = [9] RETURN n.name", {});
    expectRows("MATCH (n) WHERE n.tags <> [9] RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(ListPropertyEqualityTest, findsTheNodesHoldingNoList) {
    tagRemyAndAdam();
    expectRows("MATCH (n {name: 'Maxime'}) WHERE n.tags IS NULL RETURN n.name", {{"Maxime"}});
}

TEST_F(ListPropertyEqualityTest, findsANodeHoldingAList) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(ListPropertyEqualityTest, matchesAStoredListOnAnEdge) {
    write("MATCH (a {name: 'Remy'})-[e:KNOWS_WELL]->(b) SET e.tags = [1, 2]");
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) WHERE e.tags = [1, 2] RETURN a.name",
               {{"Remy"}});
}

// Both operands are stored lists, so neither side is a literal the analyzer can type
// from the query text.
TEST_F(ListPropertyEqualityTest, comparesTwoStoredLists) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1, 2]");
    write("MATCH (n {name: 'Adam'}) SET n.tags = [1, 2]");
    write("MATCH (n {name: 'Maxime'}) SET n.tags = [3]");
    expectRows("MATCH (a {name: 'Remy'}), (b) WHERE a.tags = b.tags RETURN b.name",
               {{"Adam"}, {"Remy"}});
}

// The unwound cell holds a list rather than being one, so the comparison meets a
// type-erased column on one side and the stored list column on the other.
TEST_F(ListPropertyEqualityTest, matchesAStoredListAgainstAnUnwoundCell) {
    tagRemyAndAdam();
    expectRows("MATCH (n) UNWIND [[1, 2]] AS candidate WITH n, candidate "
               "WHERE n.tags = candidate RETURN n.name",
               {{"Remy"}});
}

TEST_F(ListPropertyEqualityTest, doesNotMatchAnUnwoundCellHoldingAnotherList) {
    tagRemyAndAdam();
    expectRows("MATCH (n) UNWIND [[9]] AS candidate WITH n, candidate "
               "WHERE n.tags = candidate RETURN n.name",
               {});
}

// A cell holding a scalar is not a list, so it equals no stored list.
TEST_F(ListPropertyEqualityTest, doesNotMatchAnUnwoundScalarCell) {
    tagRemyAndAdam();
    expectRows("MATCH (n) UNWIND [1, [1, 2]] AS candidate WITH n, candidate "
               "WHERE n.tags = candidate RETURN n.name",
               {{"Remy"}});
}
