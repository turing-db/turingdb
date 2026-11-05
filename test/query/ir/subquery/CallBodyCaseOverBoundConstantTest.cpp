#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class CallBodyCaseOverBoundConstantTest : public WriteQueryTest {
};

TEST_F(CallBodyCaseOverBoundConstantTest, keepsTheInputRowWhenTheBodyMatchesNothing) {
    applyWrite("CREATE (:Cell {x: 0})");
    expectWriteRows("MATCH (p:Player) WITH count(p) AS k CALL { "
                    "MATCH (m)-[at:AT]->(c:Cell) WITH m, at, c, m.px + m.vx AS nx, m.py + m.vy AS ny "
                    "WITH m, at, c, nx, ny, toInteger(floor(nx)) AS tx, toInteger(floor(ny)) AS ty "
                    "OPTIONAL MATCH (c)-[:ADJ]-(n:Cell) WHERE n.x = tx AND n.y = ty "
                    "WITH m, at, nx, ny, n, tx = c.x AND ty = c.y AS stays "
                    "SET m.px = CASE WHEN stays OR n IS NOT NULL THEN nx ELSE m.px END "
                    "} RETURN k",
                    {{"0"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, keepsTheInputRowWhenStaysIsAConstant) {
    applyWrite("CREATE (:Cell {x: 0})");
    expectWriteRows("MATCH (p:Player) WITH count(p) AS k CALL { "
                    "MATCH (m)-[at:AT]->(c:Cell) WITH m, at, c, m.px + m.vx AS nx, m.py + m.vy AS ny "
                    "WITH m, at, c, nx, ny, toInteger(floor(nx)) AS tx, toInteger(floor(ny)) AS ty "
                    "OPTIONAL MATCH (c)-[:ADJ]-(n:Cell) WHERE n.x = tx AND n.y = ty "
                    "WITH m, at, nx, ny, n, false AS stays "
                    "SET m.px = CASE WHEN stays OR n IS NOT NULL THEN nx ELSE m.px END "
                    "} RETURN k",
                    {{"0"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, keepsTheInputRowWithoutTheNeighbourFilter) {
    applyWrite("CREATE (:Cell {x: 0})");
    expectWriteRows("MATCH (p:Player) WITH count(p) AS k CALL { "
                    "MATCH (m)-[at:AT]->(c:Cell) WITH m, at, c, m.px + m.vx AS nx, m.py + m.vy AS ny "
                    "WITH m, at, c, nx, ny, toInteger(floor(nx)) AS tx, toInteger(floor(ny)) AS ty "
                    "OPTIONAL MATCH (c)-[:ADJ]-(n:Cell) "
                    "WITH m, at, nx, ny, n, tx = c.x AND ty = c.y AS stays "
                    "SET m.px = CASE WHEN stays OR n IS NOT NULL THEN nx ELSE m.px END "
                    "} RETURN k",
                    {{"0"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, movesAPlayerOntoTheNeighbourItReaches) {
    applyWrite("CREATE (a:Cell {x: 0, y: 0})-[:ADJ]->(:Cell {x: 1, y: 0}) CREATE (:Player)-[:AT]->(a)");
    expectWriteRows("MATCH (p:Player) WITH count(p) AS k CALL { "
                    "MATCH (m)-[at:AT]->(c:Cell) "
                    "WITH m, at, c, coalesce(m.px, 0.5) + coalesce(m.vx, 1.0) AS nx, coalesce(m.py, 0.5) + coalesce(m.vy, 0.0) AS ny "
                    "WITH m, at, c, nx, ny, toInteger(floor(nx)) AS tx, toInteger(floor(ny)) AS ty "
                    "OPTIONAL MATCH (c)-[:ADJ]-(n:Cell) WHERE n.x = tx AND n.y = ty "
                    "WITH m, at, nx, ny, n, tx = c.x AND ty = c.y AS stays "
                    "SET m.px = CASE WHEN stays OR n IS NOT NULL THEN nx ELSE m.px END, "
                    "m.py = CASE WHEN stays OR n IS NOT NULL THEN ny ELSE m.py END "
                    "WITH m, at, n, stays WHERE NOT stays AND n IS NOT NULL "
                    "DELETE at CREATE (m)-[:AT]->(n) "
                    "} RETURN k",
                    {{"1"}});
    expectRows("MATCH (m:Player)-[:AT]->(c:Cell) RETURN m.px, m.py, c.x", {{"1.500000", "0.500000", "1"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, setsACaseOverAMissingPropertyTheBodyBinds) {
    expectWriteRows("MATCH (p:Player) WITH count(p) AS k CALL { "
                    "MATCH (m:Person) WITH m, m.nope AS nx "
                    "SET m.age = CASE WHEN nx IS NULL THEN m.age ELSE nx END "
                    "} RETURN k",
                    {{"0"}});
    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, p.age", {{"Remy", "32"}, {"Adam", "32"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, setsACaseOverALiteralTheBodyBinds) {
    expectWriteRows("MATCH (p:Player) WITH count(p) AS k CALL { "
                    "MATCH (m:Person) WITH m, 1 AS nx "
                    "SET m.age = CASE WHEN m.age > 0 THEN nx ELSE m.age END "
                    "} RETURN k",
                    {{"0"}});
    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, p.age", {{"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, setsACaseOverANullLiteralTheBodyBinds) {
    expectWriteRows("MATCH (p:Player) WITH count(p) AS k CALL { "
                    "MATCH (m:Person) WITH m, null AS nx "
                    "SET m.px = CASE WHEN m.age > 0 THEN nx ELSE m.px END "
                    "} RETURN k",
                    {{"0"}});
    expectRows("MATCH (p:Person) WHERE p.px IS NOT NULL RETURN p.name", {});
}

TEST_F(CallBodyCaseOverBoundConstantTest, setsACaseOverABoundConstantBelowAnUnwind) {
    expectWriteRows("UNWIND [1] AS k CALL { "
                    "MATCH (m:Person) WITH m, 1 AS nx "
                    "SET m.age = CASE WHEN m.age > 0 THEN nx ELSE m.age END "
                    "} RETURN k",
                    {{"1"}});
    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, p.age", {{"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, setsACaseOverABoundConstantAsTheFirstClause) {
    expectWriteRows("CALL { "
                    "MATCH (m:Person) WITH m, 1 AS nx "
                    "SET m.age = CASE WHEN m.age > 0 THEN nx ELSE m.age END "
                    "} RETURN 1",
                    {{"1"}});
    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, p.age", {{"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, setsACoalesceOverABoundConstant) {
    expectWriteRows("MATCH (p:Player) WITH count(p) AS k CALL { "
                    "MATCH (m:Person) WITH m, 1 AS nx "
                    "SET m.age = coalesce(m.nope, nx) "
                    "} RETURN k",
                    {{"0"}});
    expectRows("MATCH (p:Person) RETURN p.age, count(p)", {{"1", "8"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, setsACaseOverABoundConstantInAnImportingBody) {
    expectWriteRows("MATCH (m:Person {name: 'Remy'}) CALL (m) { "
                    "WITH m, 1 AS nx "
                    "SET m.age = CASE WHEN m.age > 0 THEN nx ELSE m.age END "
                    "} RETURN m.name",
                    {{"Remy"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"1"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, setsAMapOfACaseOverABoundConstant) {
    expectWriteRows("CALL { "
                    "MATCH (m:Person {name: 'Remy'}) WITH m, 1 AS nx "
                    "SET m += {age: CASE WHEN m.age > 0 THEN nx ELSE 0 END} "
                    "} RETURN 1",
                    {{"1"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"1"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, createsACaseOverABoundConstant) {
    expectWriteRows("CALL { "
                    "MATCH (m:Person {name: 'Remy'}) WITH m, 1 AS nx "
                    "CREATE (:Copy {age: CASE WHEN m.age > 0 THEN nx ELSE 0 END}) "
                    "} RETURN 1",
                    {{"1"}});
    expectRows("MATCH (c:Copy) RETURN c.age", {{"1"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, mergesACaseOverABoundConstant) {
    expectWriteRows("CALL { "
                    "MATCH (m:Person {name: 'Remy'}) WITH m, 1 AS nx "
                    "MERGE (q:Person {name: 'Remy'}) ON MATCH SET q.age = CASE WHEN m.age > 0 THEN nx ELSE 0 END "
                    "} RETURN 1",
                    {{"1"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"1"}});
}

TEST_F(CallBodyCaseOverBoundConstantTest, returnsACaseOverABoundConstant) {
    expectRows("CALL { "
               "MATCH (m:Person {name: 'Remy'}) WITH m, 1 AS nx "
               "RETURN CASE WHEN m.age > 0 THEN nx ELSE 0 END AS v "
               "} RETURN v",
               {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
