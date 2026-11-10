#include <gtest/gtest.h>

#include <fstream>
#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class LiteralMapKeyTest : public WriteQueryTest {
protected:
    void writeDataFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
    }
};

TEST_F(LiteralMapKeyTest, readsAKeyOfAMapVariable) {
    expectRows("WITH {a: 1, b: 'x'} AS m RETURN m['a'], m['b'], m['nosuch']", {{"1", "x", "null"}});
}

TEST_F(LiteralMapKeyTest, readsAKeyOfAMapLiteral) {
    expectRows("RETURN {a: 1}['a'], {a: 1}['nosuch']", {{"1", "null"}});
}

TEST_F(LiteralMapKeyTest, readsAKeyOfAStoredMapProperty) {
    applyWrite("CREATE (:Tagged {name: 'a', attrs: {x: 1, d: {e: 2}}})");

    expectRows("MATCH (t:Tagged) RETURN t.attrs['x'], t.attrs['nosuch'], t.attrs['d']['e'], t['attrs']['x'], t['attrs'].x",
               {{"1", "null", "2", "1", "1"}});
}

TEST_F(LiteralMapKeyTest, readsAKeyOfAMapProjection) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p {.name, .age}['age']", {{"32"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.name} AS m RETURN m['name']", {{"Remy"}});
}

TEST_F(LiteralMapKeyTest, readsAKeyOfAMapAFunctionReturns) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN properties(p)['age']", {{"32"}});
}

TEST_F(LiteralMapKeyTest, readsAKeyOfAnUnwoundMap) {
    expectRows("UNWIND [{a: 1}, {a: 2}, {b: 3}] AS m RETURN m['a']", {{"1"}, {"2"}, {"null"}});
}

TEST_F(LiteralMapKeyTest, readsAKeyOfAMapAListHolds) {
    expectRows("WITH [{a: 1}, 2] AS xs RETURN xs[0]['a']", {{"1"}});
}

TEST_F(LiteralMapKeyTest, readsAPropertyOfANode) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p['name'], p['age'], p['age'] + 1, p['nosuch']",
               {{"Remy", "32", "33", "null"}});
}

TEST_F(LiteralMapKeyTest, filtersOnAPropertyOfANode) {
    expectRows("MATCH (p:Person) WHERE p['name'] = 'Adam' RETURN p['name']", {{"Adam"}});
}

TEST_F(LiteralMapKeyTest, groupsOnAPropertyOfANode) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] RETURN p['age'] AS age, count(*) AS c",
               {{"32", "2"}});
}

TEST_F(LiteralMapKeyTest, ordersOnAPropertyOfANode) {
    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] RETURN p['name'] ORDER BY p['name'] DESC",
                      {{"Remy"}, {"Adam"}});
    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] WITH p, count(*) AS c "
                      "RETURN p['name'] ORDER BY p['name']",
                      {{"Adam"}, {"Remy"}});
}

TEST_F(LiteralMapKeyTest, matchesTheDottedFormAsAGroupingKey) {
    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] RETURN p.name AS name, count(*) AS c "
                      "ORDER BY p['name'] DESC",
                      {{"Remy", "1"}, {"Adam", "1"}});
    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] RETURN DISTINCT p['name'] AS name "
                      "ORDER BY p.name DESC",
                      {{"Remy"}, {"Adam"}});
    expectRows("WITH {a: 2} AS m RETURN m.a AS a, count(*) AS c ORDER BY m['a']", {{"2", "1"}});
}

TEST_F(LiteralMapKeyTest, readsAPropertyOfAnEdge) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e['name'], e['duration']",
               {{"Remy -> Adam", "20"}});
}

TEST_F(LiteralMapKeyTest, readsAPropertyOfAnEntityAFunctionReturns) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() RETURN startNode(e)['age'], endNode(e)['name']",
               {{"32", "Adam"}});
}

TEST_F(LiteralMapKeyTest, readsAPropertyACreateWrote) {
    expectWriteRows("CREATE (n:Fresh {a: 1}) RETURN n['a']", {{"1"}});
}

TEST_F(LiteralMapKeyTest, readsAPropertyOfAMergedNode) {
    expectWriteRows("MERGE (n:Person {name: 'Remy'}) RETURN n['age']", {{"32"}});
    expectWriteRows("MERGE (n:Person {name: 'Nobody'}) RETURN n['name']", {{"Nobody"}});
}

TEST_F(LiteralMapKeyTest, writesAPropertyFromAKeyRead) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.next = p['age'] + 1 RETURN p.next, p['next']",
                    {{"33", "33"}});
}

TEST_F(LiteralMapKeyTest, readsAPropertyOfAnEntityAMapHolds) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH {who: p} AS m RETURN m['who']['name'], m['who'].name, m.who['name']",
               {{"Remy", "Remy", "Remy"}});
}

TEST_F(LiteralMapKeyTest, readsAKeyThroughMixedChains) {
    expectRows("WITH {d: {e: 1}} AS m RETURN m.d['e'], m['d'].e, m['d']['e']", {{"1", "1", "1"}});
    expectRows("WITH {a: {b: {c: 3}}} AS m RETURN m['a']['b']['c'], m['a'].b['c'], m.a['b'].c", {{"3", "3", "3"}});
}

TEST_F(LiteralMapKeyTest, readsAFieldOfARowLoadedWithHeaders) {
    writeDataFile("headed.csv", "name,city\nAlice,London\nBob,Paris\n");

    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row RETURN row['name'], row['city']",
               {{"Alice", "London"}, {"Bob", "Paris"}});
}

TEST_F(LiteralMapKeyTest, readsNullThroughANull) {
    expectRows("WITH null AS m RETURN m['a']", {{"null"}});
    expectRows("WITH {a: null} AS m RETURN m['a']['b'], m.a['b'], m['nosuch']['b']", {{"null", "null", "null"}});
    expectRows("OPTIONAL MATCH (p:Person {name: 'Nobody'}) RETURN p['name']", {{"null"}});
}

TEST_F(LiteralMapKeyTest, raisesOnAKeyOfAValueThatHoldsNone) {
    expectError("WITH {d: 1} AS m RETURN m['d']['e']", "Index operator [] can only be applied to a list, a map, a node or a relationship, not 'Integer'");
    expectError("WITH [1, 'x'] AS xs RETURN xs[0]['a']", "Index operator [] can only be applied to a list, a map, a node or a relationship, not 'Integer'");
    expectError("UNWIND [{a: 1}, 'x'] AS m RETURN m['a']", "Index operator [] can only be applied to a list, a map, a node or a relationship, not 'String'");
}

TEST_F(LiteralMapKeyTest, raisesOnAKeyOfADatetimeACellHolds) {
    expectError("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m RETURN m['d']['year']",
                "Index operator [] can only be applied to a list, a map, a node or a relationship, not 'DateTime'");
    expectRows("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m RETURN m['d'].year", {{"2020"}});
}

TEST_F(LiteralMapKeyTest, rejectsAKeyOfAValueWithoutKeys) {
    expectError("WITH datetime('2020-03-04T00:00:00Z') AS d RETURN d['year']",
                "Index operator [] can only be applied to a list, a map, a node, a relationship or a CSV row, not 'DateTime'");
    expectError("MATCH (n:Person) RETURN n.name['x']",
                "Index operator [] can only be applied to a list, a map, a node, a relationship or a CSV row, not 'String'");
    expectError("WITH [1] AS xs RETURN xs[0]['a']",
                "Index operator [] can only be applied to a list, a map, a node, a relationship or a CSV row, not 'Integer'");
    expectError("WITH [1] AS xs RETURN xs['a']", "A list index expression must be an integer, not 'String'");
}

TEST_F(LiteralMapKeyTest, rejectsAFieldOfARowLoadedWithoutHeaders) {
    writeDataFile("plain.csv", "Alice,London\n");

    expectError("LOAD CSV 'plain.csv' AS row RETURN row['name']",
                "'row' was loaded without WITH HEADERS, so it has no field named 'name'");
}
