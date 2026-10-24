#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A property read on a value of type any reads the property of the node or the edge the
// row holds, and the key of the map it holds. Remy is 32 and Luc has no age.
class AnyEntityPropertyReadTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectWriteRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runWrite(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyEntityPropertyReadTest, readsTheNodeACaseSelects) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] "
               "WITH n, CASE WHEN n.age IS NULL THEN n ELSE {name: 'adult'} END AS v "
               "RETURN n.name, v.name",
               {{"Luc", "Luc"}, {"Remy", "adult"}});
}

TEST_F(AnyEntityPropertyReadTest, readsTheNodeACoalesceAnswers) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] "
               "WITH n, coalesce(CASE WHEN n.age IS NULL THEN null ELSE {name: 'adult'} END, n) AS v "
               "RETURN n.name, v.name",
               {{"Luc", "Luc"}, {"Remy", "adult"}});
}

TEST_F(AnyEntityPropertyReadTest, readsAPropertyTheNodeLacksAsNull) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Luc' "
               "WITH CASE WHEN true THEN n ELSE {age: 1} END AS v RETURN v.age",
               {{"null"}});
}

TEST_F(AnyEntityPropertyReadTest, readsTheEdgeACaseSelects) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "WITH CASE WHEN true THEN e ELSE {name: 'none'} END AS v RETURN v.name, v.duration",
               {{"Remy -> Adam", "20"}});
}

TEST_F(AnyEntityPropertyReadTest, readsAnUnwoundNode) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' UNWIND [n, {name: 'm'}] AS x RETURN x.name",
               {{"Remy"}, {"m"}});
}

TEST_F(AnyEntityPropertyReadTest, readsTheNodeUnderAComputedKey) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] "
               "WITH n, CASE WHEN n.age IS NULL THEN n ELSE {name: 'adult'} END AS v, 'name' AS k "
               "RETURN n.name, v[k]",
               {{"Luc", "Luc"}, {"Remy", "adult"}});
}

TEST_F(AnyEntityPropertyReadTest, readsANodeTheQueryCreated) {
    expectWriteRows("CREATE (p:Person {name: 'Zed'}) "
                    "WITH CASE WHEN true THEN p ELSE {name: 'none'} END AS v RETURN v.name",
                    {{"Zed"}});
}

TEST_F(AnyEntityPropertyReadTest, readsAPropertyTheQuerySet) {
    expectWriteRows("MATCH (n:Person) WHERE n.name = 'Luc' SET n.age = 7 "
                    "WITH CASE WHEN true THEN n ELSE {age: 0} END AS v RETURN v.age",
                    {{"7"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
