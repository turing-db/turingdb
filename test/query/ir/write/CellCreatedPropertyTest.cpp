#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A write of a value held in a type-erased cell to a property no write has typed yet. The
// property takes the widest type of the first chunk holding a value, and every later chunk
// is checked against it. Expected values are Neo4j 5.26's, except that Neo4j keeps an integer
// beside a double an integer, stores mixed kinds row by row where we raise, and rejects maps.
class CellCreatedPropertyTest : public WriteQueryTest {
protected:
    void expectWriteError(std::string_view query, std::string_view expectedError) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void writeOps(std::string_view query, std::string& ops) {
        RowSink sink;
        const QueryStatus status = runQuery("EXPLAIN (nl) " + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string program;
        for (const Row& row : sink.rows()) {
            if (row.front() == "nl") {
                program = row.back();
            }
        }

        std::istringstream lines(program);
        std::string line;
        while (std::getline(lines, line)) {
            const bool writes = line.find("nl.create_node") != std::string::npos
                             || line.find("nl.set_node_property") != std::string::npos
                             || line.find("nl.merge") != std::string::npos;
            if (writes) {
                ops += line.substr(line.find_first_not_of(' '));
                ops += '\n';
            }
        }
    }

    void expectWriteOps(std::string_view query, std::string_view expected) {
        std::string ops;
        writeOps(query, ops);

        EXPECT_EQ(ops, expected) << "query: " << query;
    }
};

TEST_F(CellCreatedPropertyTest, setsANodePropertyToAnAverageOfMixedNumbers) {
    applyWrite("UNWIND [1, 2.5] AS x WITH avg(x) AS a MATCH (n:Person {name: 'Remy'}) SET n.score = a");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.score", {{"1.750000"}});
}

TEST_F(CellCreatedPropertyTest, createsANodePropertyFromAnAverageOfMixedNumbers) {
    applyWrite("UNWIND [1, 2.5] AS x WITH avg(x) AS a CREATE (:Thing {score: a})");
    expectRows("MATCH (t:Thing) RETURN t.score", {{"1.750000"}});
}

TEST_F(CellCreatedPropertyTest, createsANodePropertyFromAKeyOfAMapLiteral) {
    applyWrite("CREATE (n:Probe {a: {x: 1 + 1}.x}) RETURN n.a");
    expectRows("MATCH (n:Probe) RETURN n.a", {{"2"}});
}

TEST_F(CellCreatedPropertyTest, setsANodePropertyToAKeyOfANestedMap) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) WITH p, {d: {e: 5}} AS m SET p.x = m.d.e");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.x", {{"5"}});
}

TEST_F(CellCreatedPropertyTest, createsANodePropertyFromASumOfMixedNumbers) {
    applyWrite("UNWIND [1, 2.5] AS x WITH sum(x) AS s CREATE (:T {s: s})");
    expectRows("MATCH (t:T) RETURN t.s", {{"3.500000"}});
}

TEST_F(CellCreatedPropertyTest, widensIntegersBesideADoubleToDoubles) {
    applyWrite("UNWIND [1, 2.5] AS x CREATE (:T {v: x})");
    expectRows("MATCH (t:T) RETURN t.v", {{"1.000000"}, {"2.500000"}});
}

TEST_F(CellCreatedPropertyTest, keepsIntegersAloneAsIntegers) {
    applyWrite("UNWIND [[1, 'a'], [2, 'b']] AS pair CREATE (:T {v: pair[0]})");
    expectRows("MATCH (t:T) RETURN t.v", {{"1"}, {"2"}});
}

TEST_F(CellCreatedPropertyTest, raisesOnACellHoldingANode) {
    expectWriteError("MATCH (n:Person {name: 'Remy'}) UNWIND [n, 1] AS x CREATE (:T {v: x})",
                     "Cannot evaluate property: unsupported type 'NodePattern'");
}

TEST_F(CellCreatedPropertyTest, createsAListProperty) {
    applyWrite("WITH [[1, 2], 'a'] AS l CREATE (:T {v: l[0]})");
    expectRows("MATCH (t:T) RETURN t.v, size(t.v)", {{"[1, 2]", "2"}});
}

TEST_F(CellCreatedPropertyTest, createsAMapProperty) {
    applyWrite("WITH [{a: 1}, 'a'] AS l CREATE (:T {v: l[0]})");
    expectRows("MATCH (t:T) RETURN t.v, t.v.a", {{"{a: 1}", "1"}});
}

TEST_F(CellCreatedPropertyTest, createsADatetimeProperty) {
    applyWrite("WITH [datetime('2020-03-04T05:06:07Z'), 'a'] AS l CREATE (:T {v: l[0]})");
    expectRows("MATCH (t:T) RETURN t.v, t.v.year", {{"2020-03-04T05:06:07Z", "2020"}});
}

TEST_F(CellCreatedPropertyTest, createsADurationProperty) {
    applyWrite("WITH [duration({hours: 25}), 'a'] AS l CREATE (:T {v: l[0]})");
    expectRows("MATCH (t:T) RETURN t.v, t.v.hours", {{"PT25H", "25"}});
}

TEST_F(CellCreatedPropertyTest, typesThePropertyByTheValuesBesideANull) {
    applyWrite("UNWIND [null, 2.5, 1] AS x CREATE (:T {v: x})");
    expectRows("MATCH (t:T) RETURN t.v", {{"null"}, {"1.000000"}, {"2.500000"}});
}

TEST_F(CellCreatedPropertyTest, leavesAChunkOfNullsToTheNextChunk) {
    _interpreter->setChunkSize(2);

    applyWrite("UNWIND [null, null, 2.5, 1] AS x CREATE (:T {v: x})");
    expectRows("MATCH (t:T) RETURN t.v", {{"null"}, {"null"}, {"1.000000"}, {"2.500000"}});
}

TEST_F(CellCreatedPropertyTest, setsNothingFromAChunkOfNulls) {
    _interpreter->setChunkSize(2);

    applyWrite("UNWIND [['Remy', null], ['Adam', null], ['Luc', 2.5], ['Maxime', 1]] AS pair "
               "MATCH (p:Person {name: pair[0]}) SET p.v = pair[1]");
    expectRows("MATCH (p:Person) WHERE p.v IS NOT NULL RETURN p.name, p.v",
               {{"Luc", "2.500000"}, {"Maxime", "1.000000"}});
}

// The one order effect: a first chunk of integers types the property Int64, so a double
// in a later chunk does not fit it
TEST_F(CellCreatedPropertyTest, raisesOnADoubleAfterAChunkOfIntegers) {
    _interpreter->setChunkSize(2);

    expectWriteError("UNWIND [1, 2, 3.5] AS x CREATE (:T {v: x})",
                     "Cannot set a property of type 'Int64' to a value of type 'Double'");
}

TEST_F(CellCreatedPropertyTest, widensIntegersAfterAChunkHoldingADouble) {
    _interpreter->setChunkSize(2);

    applyWrite("UNWIND [1.5, 2, 3] AS x CREATE (:T {v: x})");
    expectRows("MATCH (t:T) RETURN t.v", {{"1.500000"}, {"2.000000"}, {"3.000000"}});
}

TEST_F(CellCreatedPropertyTest, createsAnEdgeProperty) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "CREATE (a)-[:RATES {w: [1, 'a'][0]}]->(b)");
    expectRows("MATCH ()-[e:RATES]->() RETURN e.w", {{"1"}});
}

TEST_F(CellCreatedPropertyTest, setsAnEdgeProperty) {
    applyWrite("UNWIND [1, 2.5] AS x WITH avg(x) AS a "
               "MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e.w = a");
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.w", {{"1.750000"}});
}

TEST_F(CellCreatedPropertyTest, mergesNodesOnAPropertyTheChunkTypes) {
    applyWrite("UNWIND [1, 2.5, 1] AS x MERGE (:T {v: x})");
    expectRows("MATCH (t:T) RETURN t.v", {{"1.000000"}, {"2.500000"}});
}

TEST_F(CellCreatedPropertyTest, mergesAnEdgeOnAPropertyTheChunkTypes) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "UNWIND [[1, 'a'], [1, 'b']] AS pair "
               "MERGE (a)-[:RATES {w: pair[0]}]->(b)");
    expectRows("MATCH ()-[e:RATES]->() RETURN e.w", {{"1"}});
}

TEST_F(CellCreatedPropertyTest, mergesNodesWrittenInAnEarlierChunk) {
    _interpreter->setChunkSize(2);

    applyWrite("UNWIND [[1, 'a'], [2, 'b'], [1, 'c']] AS pair MERGE (:T {v: pair[0]})");
    expectRows("MATCH (t:T) RETURN t.v", {{"1"}, {"2"}});
}

TEST_F(CellCreatedPropertyTest, mergesANodeATypedMergeWrote) {
    applyWrite("MERGE (:T {v: 2.5}) WITH 1 AS one UNWIND [[2.5, 'a']] AS pair MERGE (:T {v: pair[0]})");
    expectRows("MATCH (t:T) RETURN t.v", {{"2.500000"}});
}

TEST_F(CellCreatedPropertyTest, mergesNothingOverAnEmptiedChunk) {
    applyWrite("UNWIND [1, 2] AS i WITH i WHERE i > 5 MERGE (:T {v: [i, 'a'][0]})");
    expectRows("MATCH (t:T) RETURN t.v", {});
}

TEST_F(CellCreatedPropertyTest, raisesOnMergingOnANullCell) {
    expectWriteError("UNWIND [[null, 'a']] AS pair MERGE (:T {v: pair[0]})",
                     "MERGE cannot match on property 'v', which is null");
}

TEST_F(CellCreatedPropertyTest, setsAPropertyOnCreate) {
    applyWrite("MERGE (t:T {k: 1}) ON CREATE SET t.v = [2.5, 'a'][0]");
    expectRows("MATCH (t:T) RETURN t.v", {{"2.500000"}});
}

TEST_F(CellCreatedPropertyTest, setsAPropertyOnMatch) {
    applyWrite("CREATE (:T {k: 1})");
    applyWrite("MERGE (t:T {k: 1}) ON MATCH SET t.v = [2, 'a'][0]");
    expectRows("MATCH (t:T) RETURN t.v", {{"2"}});
}

TEST_F(CellCreatedPropertyTest, setsATypedWriteBeforeACellWrite) {
    applyWrite("MATCH (p:Person {name: 'Remy'}), (q:Person {name: 'Adam'}) SET p.v = 1.5, q.v = [2, 'a'][0]");
    expectRows("MATCH (p:Person) WHERE p.v IS NOT NULL RETURN p.name, p.v",
               {{"Remy", "1.500000"}, {"Adam", "2.000000"}});
}

TEST_F(CellCreatedPropertyTest, setsACellWriteBeforeATypedWrite) {
    applyWrite("MATCH (p:Person {name: 'Remy'}), (q:Person {name: 'Adam'}) SET q.v = [2, 'a'][0], p.v = 1.5");
    expectRows("MATCH (p:Person) WHERE p.v IS NOT NULL RETURN p.name, p.v",
               {{"Remy", "1.500000"}, {"Adam", "2.000000"}});
}

TEST_F(CellCreatedPropertyTest, createsATypedWriteBeforeACellWrite) {
    applyWrite("CREATE (:T {v: 1.5}), (:U {v: [2, 'a'][0]})");
    expectRows("MATCH (t) WHERE t.v IS NOT NULL RETURN t.v", {{"1.500000"}, {"2.000000"}});
}

TEST_F(CellCreatedPropertyTest, createsACellWriteBeforeATypedWrite) {
    applyWrite("CREATE (:U {v: [2, 'a'][0]}), (:T {v: 1.5})");
    expectRows("MATCH (t) WHERE t.v IS NOT NULL RETURN t.v", {{"1.500000"}, {"2.000000"}});
}

TEST_F(CellCreatedPropertyTest, raisesOnACellTheTypedWriteDoesNotFit) {
    expectWriteError("MATCH (p:Person {name: 'Remy'}), (q:Person {name: 'Adam'}) SET q.v = ['x', 1][0], p.v = 1.5",
                     "Cannot set a property of type 'Double' to a value of type 'String'");
}

TEST_F(CellCreatedPropertyTest, setsAnExistingPropertyFromACell) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = [5, 'a'][0]");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"5"}});

    expectWriteError("MATCH (p:Person {name: 'Remy'}) SET p.age = [0.5, 'a'][0]",
                     "Cannot set a property of type 'Int64' to a value of type 'Double'");
}

TEST_F(CellCreatedPropertyTest, keepsTheProgramOfATypedWrite) {
    expectWriteOps("MATCH (n:Person {name: 'Remy'}) SET n.score = 1.5",
                   "nl.set_node_property %arg0, \"score\", %0 : !nl.chunk<f64>\n");
    expectWriteOps("CREATE (:Thing {score: 1.5})",
                   "%1 = nl.create_node [\"Thing\"], [\"score\"], {%0} : !nl.chunk<f64>\n");
    expectWriteOps("MERGE (:Thing {score: 1.5})",
                   "%1:3 = nl.merge nodes [[\"Thing\"]] props [[\"score\"]] edges [] props [] dirs [] "
                   "bound{} pending []{} values{%0} {} carrying{} "
                   ": (!nl.chunk<f64>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>)\n");
}

TEST_F(CellCreatedPropertyTest, removesAPropertyASetOfCellsCreated) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.v = [1, 'a'][0] REMOVE p.v");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.v", {{"null"}});
}

TEST_F(CellCreatedPropertyTest, setsNullOverAPropertyASetOfCellsCreated) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.v = [1, 'a'][0] SET p.v = null");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.v", {{"null"}});
}

TEST_F(CellCreatedPropertyTest, removesAPropertyACreateOfCellsCreated) {
    applyWrite("CREATE (n:T {v: [1, 'a'][0]}) REMOVE n.v");
    expectRows("MATCH (n:T) RETURN n.v", {{"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
