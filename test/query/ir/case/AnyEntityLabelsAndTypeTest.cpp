#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// labels() and type() read the node or the edge a value of type any holds. Remy is a
// Person, a SoftwareEngineering and a Founder, Luc a Person and a SoftwareEngineering, and
// only Remy has an age.
class AnyEntityLabelsAndTypeTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyEntityLabelsAndTypeTest, readsTheLabelsOfTheNodeACaseSelects) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] "
               "WITH n, CASE WHEN true THEN n ELSE 1 END AS v "
               "RETURN n.name, size(labels(v)), labels(v) = labels(n)",
               {{"Luc", "2", "true"}, {"Remy", "3", "true"}});
}

TEST_F(AnyEntityLabelsAndTypeTest, readsNoLabelsOfANullCell) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] "
               "WITH n, CASE WHEN n.age IS NULL THEN n WHEN false THEN 1 END AS v "
               "RETURN n.name, labels(v) IS NULL",
               {{"Luc", "false"}, {"Remy", "true"}});
}

TEST_F(AnyEntityLabelsAndTypeTest, readsTheTypeOfTheEdgeACaseSelects) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "WITH CASE WHEN true THEN e ELSE 1 END AS v RETURN type(v)",
               {{"KNOWS_WELL"}});
}

TEST_F(AnyEntityLabelsAndTypeTest, readsTheNodeAMapHolds) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Luc' WITH n, {p: n} AS m RETURN labels(m.p) = labels(n)",
               {{"true"}});
}

TEST_F(AnyEntityLabelsAndTypeTest, rejectsACellHoldingNoNode) {
    runQueryExpectingError("UNWIND [1, 'a'] AS x RETURN labels(x)", "labels() reads a node");
    runQueryExpectingError("UNWIND [1, 'a'] AS x RETURN type(x)", "type() reads a relationship");
}

TEST_F(AnyEntityLabelsAndTypeTest, readsNoCellWithoutARow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' RETURN labels([1, 'a'][0])", {});
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' RETURN type([1, 'a'][0])", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
