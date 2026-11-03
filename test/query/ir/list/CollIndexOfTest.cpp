#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// simpledb's Person nodes in id order are Remy, Adam, Maxime, Luc, Martina, Suhas, Cyrus and
// Doruk.
class CollIndexOfTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectSortedRows(std::string_view query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(CollIndexOfTest, answersThePositionOfTheFirstMatch) {
    expectRows("RETURN coll.indexOf(['a', 'b', 'c', 'c'], 'c')", {{"2"}});
    expectRows("RETURN coll.indexOf([1, 'b', false], 'b')", {{"1"}});
}

TEST_F(CollIndexOfTest, answersMinusOneWithoutAMatch) {
    expectRows("RETURN coll.indexOf([1, 'b', false], 4.3)", {{"-1"}});
    expectRows("RETURN coll.indexOf([], 1)", {{"-1"}});
}

TEST_F(CollIndexOfTest, answersNullOverANullArgument) {
    expectRows("RETURN coll.indexOf(null, 1), coll.indexOf([1], null), coll.indexOf(null, null)",
               {{"null", "null", "null"}});
}

TEST_F(CollIndexOfTest, skipsANullElement) {
    expectRows("RETURN coll.indexOf([1, null, 2], 2), coll.indexOf([1, null], 2)", {{"2", "-1"}});
}

TEST_F(CollIndexOfTest, comparesNumbersByValue) {
    expectRows("RETURN coll.indexOf([1, 2], 2.0), coll.indexOf([1.0, 2.0], 2)", {{"1", "1"}});
}

TEST_F(CollIndexOfTest, findsAListOrAMap) {
    expectRows("RETURN coll.indexOf([[1], [2, 3]], [2, 3]), coll.indexOf([{a: 1}, {a: 2}], {a: 2})",
               {{"1", "1"}});
}

TEST_F(CollIndexOfTest, findsATemporalValue) {
    expectRows("RETURN coll.indexOf([datetime('2020-01-01T00:00:00Z'), datetime('2021-01-01T00:00:00Z')], "
               "datetime('2021-01-01T00:00:00Z')), "
               "coll.indexOf([duration(4), duration(5)], duration(5))",
               {{"1", "1"}});
}

TEST_F(CollIndexOfTest, findsTheTruthOfAComparison) {
    expectRows("UNWIND [1, 5] AS x RETURN coll.indexOf([true, false], x > 2)", {{"1"}, {"0"}});
}

TEST_F(CollIndexOfTest, findsTheValueOfEveryRow) {
    expectSortedRows("MATCH (p:Person) RETURN p.name, coll.indexOf(['Adam', 'Remy'], p.name)",
                     {
                         {"Remy", "1"},
                         {"Adam", "0"},
                         {"Maxime", "-1"},
                         {"Luc", "-1"},
                         {"Martina", "-1"},
                         {"Suhas", "-1"},
                         {"Cyrus", "-1"},
                         {"Doruk", "-1"},
                     });
}

TEST_F(CollIndexOfTest, searchesTheListOfEveryRow) {
    expectRows("UNWIND [[1, 2], [2], [3], null] AS xs RETURN coll.indexOf(xs, 2)",
               {{"1"}, {"0"}, {"-1"}, {"null"}});
}

TEST_F(CollIndexOfTest, findsATaggedCell) {
    expectRows("UNWIND [1, 'a', null] AS v RETURN coll.indexOf(['a', 1], v)", {{"1"}, {"0"}, {"null"}});
}

TEST_F(CollIndexOfTest, findsANode) {
    expectRows("MATCH (a:Person) WITH collect(a) AS people "
               "MATCH (b:Person {name: 'Maxime'}) RETURN coll.indexOf(people, b)",
               {{"2"}});
}

TEST_F(CollIndexOfTest, findsAPath) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) WITH collect(p) AS paths "
               "MATCH q = (:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) RETURN coll.indexOf(paths, q)",
               {{"0"}});
}

TEST_F(CollIndexOfTest, searchesAStoredList) {
    runWrite("MATCH (n:Person {name: 'Remy'}) SET n.tags = ['x', 'y']");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN coll.indexOf(n.tags, 'y')", {{"1"}});
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN coll.indexOf(n.tags, 'y')", {{"null"}});
}

TEST_F(CollIndexOfTest, slicesAListPastTheMatch) {
    expectRows("WITH ['DID', 'RDG', 'PAD'] AS interchanges "
               "RETURN interchanges[coll.indexOf(interchanges, 'RDG') + 1..]",
               {{"PAD"}});
}

TEST_F(CollIndexOfTest, readsTheNameWhateverItsCase) {
    expectRows("RETURN COLL.INDEXOF([1, 2], 2), Coll.IndexOf([1, 2], 1)", {{"1", "0"}});
}

TEST_F(CollIndexOfTest, readsANamespaceSpelledLikeAVariable) {
    expectRows("WITH [3, 4] AS coll RETURN coll.indexOf(coll, 4)", {{"1"}});
}

TEST_F(CollIndexOfTest, rejectsAFunctionNoNamespaceHolds) {
    runQueryExpectingError("RETURN coll.find([1], 1)", "Function 'coll.find' does not exist");
    runQueryExpectingError("RETURN col.indexOf([1], 1)", "Function 'col.indexOf' does not exist");
    runQueryExpectingError("RETURN coll.indexOf.x([1], 1)", "Function 'coll.indexOf.x' does not exist");
    runQueryExpectingError("RETURN indexOf([1], 1)", "Function 'indexOf' does not exist");
    runQueryExpectingError("RETURN coll([1], 1)", "Function 'coll' does not exist");
    runQueryExpectingError("RETURN size.coll([1])", "Function 'size.coll' does not exist");
}

TEST_F(CollIndexOfTest, rejectsAValueThatIsNoList) {
    runQueryExpectingError("RETURN coll.indexOf(1, 1)", "Invalid arguments for function 'coll.indexOf'");
    runQueryExpectingError("RETURN coll.indexOf([1])", "Invalid arguments for function 'coll.indexOf'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
