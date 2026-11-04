#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A read, in the query that writes it, of a property a write of type-erased cells creates.
// It answers the value the write stored, as a read of a property a typed write creates does.
// Expected values are Neo4j 5.26's, except that we store an integer beside a double as a
// double and read it back as one: Neo4j keeps 1 an integer.
class CellCreatedPropertyReadTest : public WriteQueryTest {
protected:
    void expectWriteRowsInOrder(std::string_view query, const Rows& expected) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;

        submit(changeID);
    }
};

TEST_F(CellCreatedPropertyReadTest, returnsAPropertyACreateWrote) {
    expectWriteRows("CREATE (n:Probe {a: {x: 1 + 1}.x}) RETURN n.a", {{"2"}});
}

TEST_F(CellCreatedPropertyReadTest, returnsAPropertyASetWrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) WITH p, {d: {e: 5}} AS m SET p.x = m.d.e RETURN p.x", {{"5"}});
}

TEST_F(CellCreatedPropertyReadTest, returnsAPropertyAMergeWrote) {
    expectWriteRows("UNWIND [1, 2.5] AS x MERGE (t:T {w: x}) RETURN t.w", {{"1.000000"}, {"2.500000"}});
}

TEST_F(CellCreatedPropertyReadTest, returnsAnEdgePropertyACreateWrote) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "CREATE (a)-[e:RATES {w: [1, 'a'][0]}]->(b) RETURN e.w",
                    {{"1"}});
}

TEST_F(CellCreatedPropertyReadTest, readsThePropertyThroughAWith) {
    expectWriteRows("CREATE (n:T {v: [1, 'a'][0]}) WITH n RETURN n.v", {{"1"}});
}

TEST_F(CellCreatedPropertyReadTest, filtersOnTheProperty) {
    expectWriteRows("UNWIND [1, 2.5] AS x CREATE (n:T {v: x}) WITH n WHERE n.v > 2 RETURN n.v", {{"2.500000"}});
}

TEST_F(CellCreatedPropertyReadTest, ordersByTheProperty) {
    expectWriteRowsInOrder("UNWIND [2.5, 1] AS x CREATE (n:T {v: x}) RETURN n.v ORDER BY n.v",
                           {{"1.000000"}, {"2.500000"}});
}

TEST_F(CellCreatedPropertyReadTest, aggregatesTheProperty) {
    expectWriteRows("UNWIND [1, 2.5] AS x CREATE (n:T {v: x}) RETURN sum(n.v), count(n.v)", {{"3.500000", "2"}});
}

TEST_F(CellCreatedPropertyReadTest, setsAnotherPropertyFromIt) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.v = [1, 'a'][0] SET p.w = p.v + 1 RETURN p.w", {{"2"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.w", {{"2"}});
}

TEST_F(CellCreatedPropertyReadTest, matchesOnTheProperty) {
    expectWriteRows("CREATE (:T {v: [1, 'a'][0]}) WITH 1 AS one MATCH (t:T) WHERE t.v = 1 RETURN count(t)", {{"1"}});
    expectWriteRows("CREATE (:U {v: [1, 'a'][0]}) WITH 1 AS one MATCH (u:U {v: 1}) RETURN count(u)", {{"1"}});
}

TEST_F(CellCreatedPropertyReadTest, readsThePropertyOffAnExpression) {
    expectWriteRows("CREATE (n:T {v: [1, 'a'][0]}) WITH collect(n) AS ns RETURN head(ns).v", {{"1"}});
}

TEST_F(CellCreatedPropertyReadTest, readsNullOffANullEntity) {
    expectWriteRows("OPTIONAL MATCH (p:Person {name: 'Nobody'}) SET p.v = [1, 'a'][0] RETURN p.v", {{"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
