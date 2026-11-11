#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

class CollSortMultiTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(CollSortMultiTest, sortsAscendingOnAFieldNamedWithACaret) {
    expectRows("RETURN coll.sortMulti([{name: 'b'}, {name: 'c'}, {name: 'a'}], ['^name'])",
               {{"{name: a}, {name: b}, {name: c}"}});
}

TEST_F(CollSortMultiTest, sortsDescendingOnAFieldNamedWithoutACaret) {
    expectRows("RETURN coll.sortMulti([{name: 'b'}, {name: 'c'}, {name: 'a'}], ['name'])",
               {{"{name: c}, {name: b}, {name: a}"}});
}

TEST_F(CollSortMultiTest, breaksATieOnTheNextField) {
    expectRows("RETURN coll.sortMulti([{name: 'graphs', age: 32}, {name: 'are', age: 21}, "
               "{name: 'everywhere', age: 42}, {name: 'are', age: 50}], ['^name', 'age'])",
               {{"{age: 50, name: are}, {age: 21, name: are}, {age: 42, name: everywhere}, {age: 32, name: graphs}"}});
}

TEST_F(CollSortMultiTest, keepsTheOrderOfMapsThatTie) {
    expectRows("RETURN coll.sortMulti([{a: 1, b: 'x'}, {a: 0, b: 'y'}, {a: 1, b: 'z'}, {a: 0, b: 'w'}], ['^a'])",
               {{"{a: 0, b: y}, {a: 0, b: w}, {a: 1, b: x}, {a: 1, b: z}"}});
}

TEST_F(CollSortMultiTest, keepsTheListWithoutAField) {
    expectRows("RETURN coll.sortMulti([{a: 2}, {a: 1}]), coll.sortMulti([{a: 2}, {a: 1}], [])",
               {{"{a: 2}, {a: 1}", "{a: 2}, {a: 1}"}});
}

TEST_F(CollSortMultiTest, keepsAsManyMapsAsTheLimit) {
    expectRows("RETURN coll.sortMulti([{a: 3}, {a: 1}, {a: 2}], ['^a'], 2), "
               "coll.sortMulti([{a: 3}, {a: 1}, {a: 2}], ['^a'], -1), "
               "coll.sortMulti([{a: 3}, {a: 1}, {a: 2}], ['^a'], 0)",
               {{"{a: 1}, {a: 2}", "{a: 1}, {a: 2}, {a: 3}", ""}});
}

TEST_F(CollSortMultiTest, skipsTheFirstMaps) {
    expectRows("RETURN coll.sortMulti([{a: 3}, {a: 1}, {a: 2}, {a: 4}], ['^a'], 2, 1), "
               "coll.sortMulti([{a: 3}, {a: 1}, {a: 2}], ['^a'], -1, 2), "
               "coll.sortMulti([{a: 3}, {a: 1}, {a: 2}], ['^a'], 5, 4)",
               {{"{a: 2}, {a: 3}", "{a: 3}", ""}});
}

TEST_F(CollSortMultiTest, sortsAMissingKeyBeforeEveryValue) {
    expectRows("RETURN coll.sortMulti([{a: 2}, {b: 1}, {a: null}, {a: 1}], ['^a']), "
               "coll.sortMulti([{a: 2}, {b: 1}, {a: 1}], ['a'])",
               {{"{b: 1}, {a: null}, {a: 1}, {a: 2}", "{a: 2}, {a: 1}, {b: 1}"}});
}

TEST_F(CollSortMultiTest, ordersValuesOfDifferentTypes) {
    expectRows("RETURN coll.sortMulti([{v: 2.5}, {v: 'a'}, {v: 1}, {v: true}], ['^v'])",
               {{"{v: a}, {v: true}, {v: 1}, {v: 2.5}"}});
}

TEST_F(CollSortMultiTest, answersNullOverANullArgument) {
    expectRows("RETURN coll.sortMulti(null, ['^a']), coll.sortMulti([{a: 1}], null), "
               "coll.sortMulti([{a: 1}], ['^a'], null), coll.sortMulti([{a: 1}], ['^a'], 1, null)",
               {{"null", "null", "null", "null"}});
}

TEST_F(CollSortMultiTest, sortsTheListOfEveryRow) {
    expectRows("UNWIND [[{a: 2}, {a: 1}], [{a: 3}, {a: 4}], null] AS xs RETURN coll.sortMulti(xs, ['a'])",
               {{"{a: 2}, {a: 1}"}, {"{a: 4}, {a: 3}"}, {"null"}});
}

TEST_F(CollSortMultiTest, sortsAConstantListOnceForEveryRow) {
    StringRowSink sink;
    runQuery("EXPLAIN (nl) MATCH (n:Person) RETURN coll.sortMulti([{a: 2}, {a: 1}], ['^a'])", sink);

    std::string_view nl;
    for (const StringRowSink::Row& row : sink.getRows()) {
        if (row.front() == "nl") {
            nl = row.back();
        }
    }

    const size_t sort = nl.find("nl.sort_multi");
    ASSERT_NE(sort, std::string_view::npos) << nl;
    EXPECT_LT(sort, nl.find("nl.for")) << nl;

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN coll.sortMulti([{a: 2}, {a: 1}], ['^a'])",
               {{"{a: 1}, {a: 2}"}});
}

TEST_F(CollSortMultiTest, sortsAListHeldInACell) {
    expectRows("UNWIND [[{a: 2}, {a: 1}], 'x'] AS xs WITH xs WHERE xs <> 'x' RETURN coll.sortMulti(xs, ['^a'])",
               {{"{a: 1}, {a: 2}"}});

    runQueryExpectingError("UNWIND [[{a: 2}, {a: 1}], 1] AS xs RETURN coll.sortMulti(xs, ['a'])",
                           "reads a list, not 'Integer'");
}

TEST_F(CollSortMultiTest, sortsTheListOfEveryRowAcrossChunks) {
    expectRows("UNWIND range(1, 70000) AS i "
               "WITH i, coll.sortMulti([{a: i}, {a: 0}, {a: -i}], ['a']) AS sorted "
               "RETURN count(*), sum(sorted[0].a), sum(sorted[2].a), "
               "sum(size(coll.sortMulti([{a: i}, {a: 0}, {a: -i}], ['a'], -1, i % 3)))",
               {{"70000", "2450035000", "-2450035000", "140000"}});
}

TEST_F(CollSortMultiTest, readsTheFieldsAndBoundsOfEveryRow) {
    expectRows("UNWIND [['^a', 1], ['a', 0]] AS spec "
               "RETURN coll.sortMulti([{a: 1}, {a: 3}, {a: 2}], [spec[0]], 2, spec[1])",
               {{"{a: 2}, {a: 3}"}, {"{a: 3}, {a: 2}"}});
}

TEST_F(CollSortMultiTest, sortsCollectedMaps) {
    expectRows("MATCH (p:Person) WITH collect({name: p.name}) AS people "
               "RETURN coll.sortMulti(people, ['^name'], 3)",
               {{"{name: Adam}, {name: Cyrus}, {name: Doruk}"}});
}

TEST_F(CollSortMultiTest, sortsAStoredList) {
    runWrite("MATCH (n:Person {name: 'Remy'}) SET n.items = [{a: 2}, {a: 1}, {a: 3}]");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN coll.sortMulti(n.items, ['^a'])",
               {{"{a: 1}, {a: 2}, {a: 3}"}});
}

TEST_F(CollSortMultiTest, unwindsTheSortedMaps) {
    expectRows("UNWIND coll.sortMulti([{a: 2}, {a: 1}, {a: 3}], ['^a']) AS m RETURN m.a",
               {{"1"}, {"2"}, {"3"}});
}

TEST_F(CollSortMultiTest, readsTheNameWhateverItsCase) {
    expectRows("RETURN COLL.SORTMULTI([{a: 1}, {a: 2}], ['a'])", {{"{a: 2}, {a: 1}"}});
}

TEST_F(CollSortMultiTest, keepsAnyListWithoutAField) {
    expectRows("RETURN coll.sortMulti([{a: 1}, null, 2], [], 2), "
               "CASE WHEN false THEN coll.sortMulti([{a: 1}, null], ['a']) END",
               {{"{a: 1}, null", "null"}});
}

TEST_F(CollSortMultiTest, sortsANullElementAsAMapHoldingNoKey) {
    expectRows("RETURN coll.sortMulti([{a: 1}, null, {a: 0}], ['^a']), coll.sortMulti([{a: 1}, null, {a: 0}], ['a']), "
               "CASE WHEN false THEN coll.sortMulti([{a: 1}, 2], ['a']) END",
               {{"null, {a: 0}, {a: 1}", "{a: 1}, {a: 0}, null", "null"}});
}

TEST_F(CollSortMultiTest, raisesOnAnElementThatIsNoMap) {
    runQueryExpectingError("RETURN coll.sortMulti([{a: 1}, 2], ['^a'])", "coll.sortMulti sorts a list of maps");
}

TEST_F(CollSortMultiTest, raisesOnAFieldThatIsNoString) {
    runQueryExpectingError("RETURN coll.sortMulti([{a: 1}], [1])", "coll.sortMulti sorts on fields named by strings");
}

TEST_F(CollSortMultiTest, raisesOnANegativeSkip) {
    runQueryExpectingError("RETURN coll.sortMulti([{a: 1}], ['^a'], -1, -1)", "coll.sortMulti cannot skip a negative count");
}

TEST_F(CollSortMultiTest, rejectsAValueThatIsNoList) {
    runQueryExpectingError("RETURN coll.sortMulti(1)", "Invalid arguments for function 'coll.sortMulti'");
    runQueryExpectingError("RETURN coll.sortMulti([{a: 1}], 'a')", "Invalid arguments for function 'coll.sortMulti'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
