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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// sum over a map value, a stored list element or an element of a mixed list reads cells
// that carry their type per row. Integers sum to an integer, as they do in a typed column,
// and the sum is a float only once a float is summed. RowSink writes an integer as 7 and a
// float as 7.000000, so each expected row pins the type as well as the value.
class TaggedCellSumTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

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
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_env->getCompilerContext(),
                                     &_queryConfig,
                                     nullptr,
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

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(TaggedCellSumTest, sumsOneMapIntegerToAnInteger) {
    expectRows("WITH {id: 7} AS m RETURN sum(m.id)", {{"7"}});
}

TEST_F(TaggedCellSumTest, sumsAnExpressionOverMapIntegersToAnInteger) {
    expectRows("UNWIND [1, 2, 3] AS x WITH {id: x} AS m RETURN sum(m.id % 10)", {{"6"}});
}

TEST_F(TaggedCellSumTest, sumsMapIntegersAbove2To53Exactly) {
    expectRows("UNWIND [9007199254740993, 1] AS x WITH {id: x} AS m RETURN sum(m.id)", {{"9007199254740994"}});
}

TEST_F(TaggedCellSumTest, sumsMapIntegersOfMatchedNodes) {
    expectRows("MATCH (n:Person) WITH {age: n.age} AS m RETURN sum(m.age)", {{"64"}});
}

TEST_F(TaggedCellSumTest, sumsIntegersOfAListOfMaps) {
    expectRows("UNWIND [{v: 1}, {v: 2}] AS m RETURN sum(m.v)", {{"3"}});
}

TEST_F(TaggedCellSumTest, skipsANullMapValue) {
    expectRows("UNWIND [{v: 1}, {v: null}, {v: 3}] AS m RETURN sum(m.v)", {{"4"}});
}

TEST_F(TaggedCellSumTest, sumsOnlyNullMapValuesToTheIntegerZero) {
    expectRows("UNWIND [{v: null}, {v: null}] AS m RETURN sum(m.v)", {{"0"}});
}

TEST_F(TaggedCellSumTest, sumsAnIntegerAndAFloatToAFloat) {
    expectRows("UNWIND [{v: 1}, {v: 2.5}] AS m RETURN sum(m.v)", {{"3.500000"}});
}

TEST_F(TaggedCellSumTest, sumsAMixedListToAFloat) {
    expectRows("UNWIND [1, 2.5] AS x RETURN sum(x)", {{"3.500000"}});
}

TEST_F(TaggedCellSumTest, sumsWholeFloatsToAFloat) {
    expectRows("UNWIND [{v: 1.0}, {v: 2.0}] AS m RETURN sum(m.v)", {{"3.000000"}});
}

TEST_F(TaggedCellSumTest, sumsTheDistinctMapIntegersToAnInteger) {
    expectRows("UNWIND [1, 1, 2] AS x WITH {id: x} AS m RETURN sum(DISTINCT m.id)", {{"3"}});
}

TEST_F(TaggedCellSumTest, addsAnIntegerToAnIntegerSum) {
    expectRows("UNWIND [1, 2] AS x WITH {id: x} AS m RETURN sum(m.id) + 1", {{"4"}});
}

TEST_F(TaggedCellSumTest, sumsMapIntegersWithinEachGroup) {
    expectRows("UNWIND [{k: 0, v: 1}, {k: 0, v: 2}, {k: 1, v: 3}, {k: 1, v: 4}] AS m RETURN m.k, sum(m.v)",
               {{"0", "3"}, {"1", "7"}});
}

TEST_F(TaggedCellSumTest, sumsToAFloatOnlyInTheGroupHoldingAFloat) {
    expectRows("UNWIND [{k: 0, v: 1}, {k: 0, v: 2}, {k: 1, v: 3}, {k: 1, v: 0.5}] AS m RETURN m.k, sum(m.v)",
               {{"0", "3"}, {"1", "3.500000"}});
}

TEST_F(TaggedCellSumTest, sumsAGroupOfOnlyNullsToTheIntegerZero) {
    expectRows("UNWIND [{k: 0, v: 1}, {k: 1, v: null}] AS m RETURN m.k, sum(m.v)",
               {{"0", "1"}, {"1", "0"}});
}

TEST_F(TaggedCellSumTest, sumsMapIntegersAbove2To53ExactlyWithinAGroup) {
    expectRows("UNWIND [{k: 0, v: 9007199254740993}, {k: 0, v: 1}] AS m RETURN m.k, sum(m.v)",
               {{"0", "9007199254740994"}});
}

TEST_F(TaggedCellSumTest, sumsTheDistinctMapIntegersWithinAGroup) {
    expectRows("UNWIND [{k: 0, v: 1}, {k: 0, v: 1}, {k: 0, v: 2}] AS m RETURN m.k, sum(DISTINCT m.v)",
               {{"0", "3"}});
}

TEST_F(TaggedCellSumTest, sumsMapIntegersWithinEachMatchedNode) {
    expectRows("MATCH (n:Person) WITH n, {age: n.age} AS m RETURN n.name, sum(m.age)",
               {{"Adam", "32"},
                {"Cyrus", "0"},
                {"Doruk", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Maxime", "0"},
                {"Remy", "32"},
                {"Suhas", "0"}});
}

TEST_F(TaggedCellSumTest, sumsAStoredMapPropertyValue) {
    write("MATCH (p:Person {name: 'Remy'}) SET p.attrs = {w: 9007199254740993}");
    write("MATCH (p:Person {name: 'Adam'}) SET p.attrs = {w: 1}");

    expectRows("MATCH (p:Person) RETURN sum(p.attrs.w)", {{"9007199254740994"}});
}

TEST_F(TaggedCellSumTest, sumsAStoredListElement) {
    write("MATCH (p:Person {name: 'Remy'}) SET p.scores = [9007199254740993, 'a']");
    write("MATCH (p:Person {name: 'Adam'}) SET p.scores = [1]");

    expectRows("MATCH (p:Person) RETURN sum(p.scores[0])", {{"9007199254740994"}});
}

TEST_F(TaggedCellSumTest, sumsMapIntegersAcrossManyChunks) {
    expectRows("UNWIND range(1, 100000) AS x WITH {v: x} AS m RETURN sum(m.v)", {{"5000050000"}});
}

TEST_F(TaggedCellSumTest, sumsToAFloatWhenTheFloatComesInALaterChunk) {
    expectRows("UNWIND range(1, 10000) AS x WITH {v: CASE WHEN x = 10000 THEN 0.5 ELSE x END} AS m RETURN sum(m.v)",
               {{"49995000.500000"}});
}

TEST_F(TaggedCellSumTest, sumsMapIntegersWithinEachGroupAcrossManyChunks) {
    expectRows("UNWIND range(1, 10000) AS x WITH x % 7 AS k, {v: x} AS m RETURN k, sum(m.v)",
               {{"0", "7142142"},
                {"1", "7143571"},
                {"2", "7145000"},
                {"3", "7146429"},
                {"4", "7147858"},
                {"5", "7139286"},
                {"6", "7140714"}});
}

TEST_F(TaggedCellSumTest, sumsMapIntegersBesideACollect) {
    expectRows("UNWIND [{k: 0, v: 1}, {k: 0, v: 2}, {k: 1, v: 0.5}] AS m RETURN m.k, collect(m.k), sum(m.v)",
               {{"0", "[0, 0]", "3"}, {"1", "[1]", "0.500000"}});
}

TEST_F(TaggedCellSumTest, filtersOnAnIntegerSum) {
    expectRows("UNWIND [1, 2] AS x WITH {id: x} AS m WITH sum(m.id) AS s WHERE s = 3 RETURN s", {{"3"}});
}

TEST_F(TaggedCellSumTest, ordersTheGroupsByTheirSum) {
    expectOrderedRows("UNWIND [{k: 0, v: 5}, {k: 1, v: 1}, {k: 1, v: 1.5}, {k: 2, v: 3}] AS m "
                      "RETURN m.k AS k, sum(m.v) AS s ORDER BY s DESC",
                      {{"0", "5"}, {"2", "3"}, {"1", "2.500000"}});
}

TEST_F(TaggedCellSumTest, rejectsACellHoldingNoNumber) {
    expectError("UNWIND [{v: 1}, {v: 'a'}] AS m RETURN sum(m.v)", "requires a numeric column");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
