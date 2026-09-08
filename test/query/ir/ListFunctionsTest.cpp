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

// size(), head() and tail() over the two kinds of list a query holds: the literal one it
// spells out, which rides a constant cell, and the stored one a property fetch reads back
// out of a datapart, which rides a nullable cell. Each write runs in its own change and is
// submitted, so a read sees a committed value rather than the writing query's own column.
class ListFunctionsTest : public TuringTest {
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

    void expectRejected(std::string_view query, std::string_view messagePart) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was accepted";
        EXPECT_NE(status.getError().find(messagePart), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(ListFunctionsTest, sizesALiteralList) {
    expectRows("RETURN size([1, 2, 3])", {{"3"}});
}

TEST_F(ListFunctionsTest, sizesAnEmptyLiteralList) {
    expectRows("RETURN size([])", {{"0"}});
}

TEST_F(ListFunctionsTest, sizesAHeterogeneousLiteralList) {
    expectRows("RETURN size([1, 'two', true])", {{"3"}});
}

TEST_F(ListFunctionsTest, sizesAStoredList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");
    expectRows("MATCH (n:Tagged) RETURN size(n.tags)", {{"3"}});
}

TEST_F(ListFunctionsTest, sizesAStoredNestedListByItsOuterElements) {
    write("CREATE (n:Tagged {name: 'a', tags: [[1, 2], [3], [4, 5, 6]]})");
    expectRows("MATCH (n:Tagged) RETURN size(n.tags)", {{"3"}});
}

TEST_F(ListFunctionsTest, sizesNullWhereTheNodeHasNoList) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b'})");

    expectRows("MATCH (n:Tagged) RETURN n.name, size(n.tags)",
               {{"a", "2"}, {"b", "null"}});
}

TEST_F(ListFunctionsTest, sizesACollectedList) {
    expectRows("MATCH (n:Person) RETURN size(collect(n.name))", {{"8"}});
}

TEST_F(ListFunctionsTest, headsALiteralList) {
    expectRows("RETURN head([1, 2, 3])", {{"1"}});
}

TEST_F(ListFunctionsTest, headsAStringList) {
    expectRows("RETURN head(['x', 'yy'])", {{"x"}});
}

TEST_F(ListFunctionsTest, headsAnEmptyListIntoNull) {
    expectRows("RETURN head([])", {{"null"}});
}

TEST_F(ListFunctionsTest, headsAHeterogeneousListByTheElementTag) {
    expectRows("RETURN head([true, 'two', 3])", {{"true"}});
}

TEST_F(ListFunctionsTest, headsANestedListIntoItsFirstList) {
    expectRows("RETURN head([[1, 2], [3]])", {{"[1, 2]"}});
}

TEST_F(ListFunctionsTest, headsAListStartingWithNull) {
    expectRows("RETURN head([null, 2])", {{"null"}});
}

TEST_F(ListFunctionsTest, headsAStoredList) {
    write("CREATE (n:Tagged {name: 'a', tags: ['x', 'yy']})");
    expectRows("MATCH (n:Tagged) RETURN head(n.tags)", {{"x"}});
}

TEST_F(ListFunctionsTest, headsNullWhereTheNodeHasNoList) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b', tags: []})");
    write("CREATE (c:Tagged {name: 'c'})");

    expectRows("MATCH (n:Tagged) RETURN n.name, head(n.tags)",
               {{"a", "1"}, {"b", "null"}, {"c", "null"}});
}

TEST_F(ListFunctionsTest, headsACollectedList) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN head(collect(n.name))", {{"Remy"}});
}

TEST_F(ListFunctionsTest, testsTheHeadOfAnEmptyListForNull) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b', tags: []})");
    write("CREATE (c:Tagged {name: 'c'})");

    expectRows("MATCH (n:Tagged) WHERE head(n.tags) IS NULL RETURN n.name",
               {{"b"}, {"c"}});
    expectRows("MATCH (n:Tagged) WHERE head(n.tags) IS NOT NULL RETURN n.name", {{"a"}});
}

TEST_F(ListFunctionsTest, testsAHeadHoldingNullForNull) {
    write("CREATE (a:Tagged {name: 'a', tags: [null, 2]})");
    write("CREATE (b:Tagged {name: 'b', tags: [3]})");

    expectRows("MATCH (n:Tagged) WHERE head(n.tags) IS NULL RETURN n.name", {{"a"}});
}

TEST_F(ListFunctionsTest, testsTheHeadOfALiteralListForNull) {
    expectRows("RETURN head([]) IS NULL, head([1]) IS NULL", {{"true", "false"}});
}

TEST_F(ListFunctionsTest, testsAnUnwoundElementForNull) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, null, 'three']})");

    expectRows("MATCH (n:Tagged) UNWIND n.tags AS tag WITH tag WHERE tag IS NULL "
               "RETURN count(*)",
               {{"1"}});
}

TEST_F(ListFunctionsTest, comparesAHeadAgainstAValue) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b', tags: [2, 1]})");

    expectRows("MATCH (n:Tagged) WHERE head(n.tags) = 1 RETURN n.name", {{"a"}});
}

TEST_F(ListFunctionsTest, countsTheNonNullHeadsOfStoredLists) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b', tags: []})");
    write("CREATE (c:Tagged {name: 'c', tags: [3]})");

    expectRows("MATCH (n:Tagged) RETURN count(head(n.tags))", {{"2"}});
}

TEST_F(ListFunctionsTest, tailsALiteralList) {
    expectRows("RETURN tail([1, 2, 3])", {{"[2, 3]"}});
}

TEST_F(ListFunctionsTest, tailsASingletonIntoAnEmptyList) {
    expectRows("RETURN tail([1])", {{"[]"}});
}

TEST_F(ListFunctionsTest, tailsAnEmptyListIntoAnEmptyList) {
    expectRows("RETURN tail([])", {{"[]"}});
}

TEST_F(ListFunctionsTest, tailsAHeterogeneousList) {
    expectRows("RETURN tail([1, 'two', true])", {{"[two, true]"}});
}

TEST_F(ListFunctionsTest, tailsANestedList) {
    expectRows("RETURN tail([[1, 2], [3]])", {{"[[3]]"}});
}

TEST_F(ListFunctionsTest, tailsAStoredList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");
    expectRows("MATCH (n:Tagged) RETURN tail(n.tags)", {{"[2, 3]"}});
}

TEST_F(ListFunctionsTest, tailsNullWhereTheNodeHasNoList) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b'})");

    expectRows("MATCH (n:Tagged) RETURN n.name, tail(n.tags)",
               {{"a", "[2]"}, {"b", "null"}});
}

TEST_F(ListFunctionsTest, readsALiteralListInOneProjection) {
    expectRows("RETURN head([1, 2, 3, 4]), size([1, 2, 3, 4]), tail([1, 2, 3, 4])",
               {{"1", "4", "[2, 3, 4]"}});
}

TEST_F(ListFunctionsTest, readsAListBoundByWith) {
    expectRows("WITH [1, 2, 3, 4] AS l RETURN head(l), size(l), tail(l)",
               {{"1", "4", "[2, 3, 4]"}});
}

TEST_F(ListFunctionsTest, readsAStoredListBoundByWith) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");
    expectRows("MATCH (n:Tagged) WITH n.tags AS l RETURN head(l), size(l), tail(l)",
               {{"1", "3", "[2, 3]"}});
}

TEST_F(ListFunctionsTest, readsACollectedListBoundByWith) {
    expectRows("MATCH (n:Person) WITH collect(n.name) AS l RETURN size(l)", {{"8"}});
}

// An unwind of a list of lists hands each nested list on as a tagged cell rather than as
// a list column of its own, so the functions read the list back out of the cell.
TEST_F(ListFunctionsTest, readsANestedListUnwoundAsATaggedCell) {
    expectRows("UNWIND [[1, 2], [3]] AS l RETURN head(l), size(l)",
               {{"1", "2"}, {"3", "1"}});
    expectRows("UNWIND [[1, 2, 3], [4]] AS l RETURN tail(l)",
               {{"[2, 3]"}, {"[]"}});
    expectRows("UNWIND [[1, 2], [3]] AS l RETURN size(tail(l))", {{"1"}, {"0"}});
}

// A stored list names no element type, so unwinding one binds a tagged cell whatever it
// holds: the functions read the list back out of the cell, per row.
TEST_F(ListFunctionsTest, readsAStoredNestedListUnwoundAsATaggedCell) {
    write("CREATE (n:Tagged {name: 'a', tags: [[1, 2], [3]]})");

    expectRows("MATCH (n:Tagged) UNWIND n.tags AS l RETURN size(l)", {{"2"}, {"1"}});
    expectRows("MATCH (n:Tagged) UNWIND n.tags AS l RETURN head(l)", {{"1"}, {"3"}});
    expectRows("MATCH (n:Tagged) UNWIND n.tags AS l RETURN tail(l)", {{"[2]"}, {"[]"}});
}

TEST_F(ListFunctionsTest, readsANullCellAsNull) {
    write("CREATE (n:Tagged {name: 'a', tags: [[1, 2], null]})");

    expectRows("MATCH (n:Tagged) UNWIND n.tags AS l RETURN size(l)", {{"2"}, {"null"}});
    expectRows("MATCH (n:Tagged) UNWIND n.tags AS l RETURN head(l)", {{"1"}, {"null"}});
    expectRows("MATCH (n:Tagged) UNWIND n.tags AS l RETURN tail(l)", {{"[2]"}, {"null"}});
}

TEST_F(ListFunctionsTest, reportsACellHoldingNoList) {
    write("CREATE (n:Tagged {name: 'a', tags: [[1, 2], 3]})");

    expectRejected("MATCH (n:Tagged) UNWIND n.tags AS l RETURN size(l)",
                   "read a list, and this row holds a value that is not one");
}

TEST_F(ListFunctionsTest, unwindsTheTailOfATaggedCell) {
    write("CREATE (n:Tagged {name: 'a', tags: [[1, 2, 3]]})");

    expectRows("MATCH (n:Tagged) UNWIND n.tags AS l UNWIND tail(l) AS element RETURN element",
               {{"2"}, {"3"}});
}

TEST_F(ListFunctionsTest, readsATaggedCellHeadedOutOfALiteral) {
    expectRows("RETURN size(head([[1, 2, 3], [4]])), head(head([[1, 2, 3], [4]]))",
               {{"3", "1"}});
    expectRows("RETURN tail(head([[1, 2, 3], [4]]))", {{"[2, 3]"}});
}

TEST_F(ListFunctionsTest, nestsTheListFunctions) {
    expectRows("RETURN head(tail([1, 2, 3])), size(tail([1, 2, 3]))", {{"2", "2"}});
}

TEST_F(ListFunctionsTest, sizesTheTailOfAStoredList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");
    expectRows("MATCH (n:Tagged) RETURN size(tail(n.tags))", {{"2"}});
}

TEST_F(ListFunctionsTest, unwindsTheTailOfALiteralList) {
    expectRows("UNWIND tail([1, 2, 3]) AS element RETURN element",
               {{"2"}, {"3"}});
}

TEST_F(ListFunctionsTest, unwindsTheTailOfAStoredList) {
    write("CREATE (n:Tagged {name: 'a', tags: ['x', 'yy', 'zzz']})");
    expectRows("MATCH (n:Tagged) UNWIND tail(n.tags) AS element RETURN element",
               {{"yy"}, {"zzz"}});
}

TEST_F(ListFunctionsTest, collectsTheHeadOfEachStoredList) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b', tags: [3]})");

    expectRows("MATCH (n:Tagged) RETURN size(collect(head(n.tags)))", {{"2"}});
}

TEST_F(ListFunctionsTest, ordersStoredListsByTheirHead) {
    write("CREATE (a:Tagged {name: 'a', tags: [2, 9]})");
    write("CREATE (b:Tagged {name: 'b', tags: [1, 9]})");

    RowSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "MATCH (n:Tagged) RETURN n.name ORDER BY head(n.tags)",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &_env->getMem(),
                          &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    EXPECT_EQ(sink.rows(), Rows({{"b"}, {"a"}}));
}

TEST_F(ListFunctionsTest, rejectsAListFunctionOnANonList) {
    expectRejected("MATCH (n:Person) RETURN size(n.name)", "size");
    expectRejected("MATCH (n:Person) RETURN head(n.age)", "head");
    expectRejected("MATCH (n:Person) RETURN tail(n.name)", "tail");
}
