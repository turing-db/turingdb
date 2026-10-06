#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class KeysAndPropertiesTest : public WriteQueryTest {
};

TEST_F(KeysAndPropertiesTest, keysOfANode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN keys(n)",
               {{"[age, dob, hasPhD, isFrench, name]"}});
}

TEST_F(KeysAndPropertiesTest, keysOfEveryPerson) {
    expectRows("MATCH (n:Person) RETURN n.name, keys(n)", {
        {"Adam", "[age, dob, hasPhD, isFrench, name]"},
        {"Cyrus", "[hasPhD, isFrench, name]"},
        {"Doruk", "[hasPhD, isFrench, name]"},
        {"Luc", "[dob, hasPhD, isFrench, name]"},
        {"Martina", "[hasPhD, isFrench, name]"},
        {"Maxime", "[dob, hasPhD, isFrench, name]"},
        {"Remy", "[age, dob, hasPhD, isFrench, name]"},
        {"Suhas", "[hasPhD, isFrench, name]"},
    });
}

TEST_F(KeysAndPropertiesTest, unwindsTheKeys) {
    expectRows("MATCH (n:Person {name: 'Martina'}) UNWIND keys(n) AS k RETURN k",
               {{"hasPhD"}, {"isFrench"}, {"name"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAnEdge) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e]->(m) RETURN m.name, keys(e)", {
        {"Adam", "[duration, name]"},
        {"Computers", "[name, proficiency]"},
        {"Eighties", "[duration, name, proficiency]"},
        {"Ghosts", "[duration, name, proficiency]"},
    });
}

TEST_F(KeysAndPropertiesTest, propertiesOfANode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN properties(n)",
               {{"{age: 32, dob: 18/01, hasPhD: true, isFrench: true, name: Remy}"}});
}

TEST_F(KeysAndPropertiesTest, propertiesOfAnEdge) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e]->(:Interest {name: 'Ghosts'}) RETURN properties(e)",
               {{"{duration: 20, name: Remy -> Ghosts, proficiency: expert}"}});
}

TEST_F(KeysAndPropertiesTest, readsAKeyOfTheProperties) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH properties(n) AS p RETURN p.age, p.name",
               {{"32", "Remy"}});
    expectRows("MATCH (:Person {name: 'Remy'})-[e]->(m) RETURN m.name, properties(e).duration", {
        {"Adam", "20"},
        {"Computers", "null"},
        {"Eighties", "20"},
        {"Ghosts", "20"},
    });
}

TEST_F(KeysAndPropertiesTest, groupsOnTheKeys) {
    expectRows("MATCH (n:Person) RETURN keys(n), count(*)", {
        {"[age, dob, hasPhD, isFrench, name]", "2"},
        {"[dob, hasPhD, isFrench, name]", "2"},
        {"[hasPhD, isFrench, name]", "4"},
    });
}

TEST_F(KeysAndPropertiesTest, filtersOnTheKeyCount) {
    expectRows("MATCH (n:Person) WHERE size(keys(n)) = 5 RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(KeysAndPropertiesTest, readsNullWhereAnOptionalMatchMissed) {
    expectRows("MATCH (n:Person {name: 'Cyrus'}) OPTIONAL MATCH (n)-[e]->(m:Person) "
               "RETURN keys(e), properties(e), keys(m), properties(m)",
               {{"null", "null", "null", "null"}});
}

TEST_F(KeysAndPropertiesTest, readsNullOverNull) {
    expectRows("RETURN keys(null), properties(null)", {{"null", "null"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAMapLiteral) {
    expectRows("RETURN keys({b: 1, a: 'x'})", {{"[a, b]"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAMapBuiltPerRow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN keys({y: n.age, x: n.name})", {{"[x, y]"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAnEmptyMap) {
    expectRows("RETURN keys({})", {{"[]"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAMapValue) {
    expectRows("WITH {inner: {b: 1, a: 2}} AS m RETURN keys(m.inner), properties(m.inner), keys(m.missing)",
               {{"[a, b]", "{a: 2, b: 1}", "null"}});
}

TEST_F(KeysAndPropertiesTest, keysOfANodeHeldByAMap) {
    expectRows("MATCH (n:Person {name: 'Martina'}) WITH {p: n} AS m RETURN keys(m.p)",
               {{"[hasPhD, isFrench, name]"}});
}

TEST_F(KeysAndPropertiesTest, propertiesOfAMap) {
    expectRows("RETURN properties({b: 1, a: 'x'})", {{"{a: x, b: 1}"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAStoredMap) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {b: 1, a: 2}");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN keys(n.attrs), keys(n)",
               {{"[a, b]", "[age, attrs, dob, hasPhD, isFrench, name]"}});
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN keys(n.attrs)", {{"null"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAStoredNestedMap) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {inner: {y: 1, x: 2}}");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN keys(n.attrs.inner), properties(n.attrs.inner)",
               {{"[x, y]", "{x: 2, y: 1}"}});
}

TEST_F(KeysAndPropertiesTest, propertiesHoldAStoredListAndMap) {
    applyWrite("MATCH (n:Person {name: 'Luc'}) SET n.tags = [1, 'a'], n.attrs = {k: true}");

    expectRows("MATCH (n:Person {name: 'Luc'}) RETURN properties(n)",
               {{"{attrs: {k: true}, dob: 28/05, hasPhD: true, isFrench: true, name: Luc, tags: [1, a]}"}});
}

TEST_F(KeysAndPropertiesTest, readsTheNewestValueOfAProperty) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.age = 33");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN keys(n), properties(n)",
               {{"[age, dob, hasPhD, isFrench, name]",
                 "{age: 33, dob: 18/01, hasPhD: true, isFrench: true, name: Remy}"}});
}

TEST_F(KeysAndPropertiesTest, dropsARemovedProperty) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) REMOVE n.age");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN keys(n), properties(n)",
               {{"[dob, hasPhD, isFrench, name]",
                 "{dob: 18/01, hasPhD: true, isFrench: true, name: Remy}"}});
}

TEST_F(KeysAndPropertiesTest, keysOfANodeWithNoProperty) {
    applyWrite("CREATE (:Empty)");

    expectRows("MATCH (n:Empty) RETURN keys(n), properties(n)", {{"[]", "{}"}});
}

TEST_F(KeysAndPropertiesTest, seesAPropertyTheQuerySet) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n.title = 'cto', n.age = 40 RETURN keys(n), properties(n)",
                    {{"[age, dob, hasPhD, isFrench, name, title]",
                      "{age: 40, dob: 18/01, hasPhD: true, isFrench: true, name: Remy, title: cto}"}});
}

TEST_F(KeysAndPropertiesTest, dropsAPropertyTheQueryRemoved) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) REMOVE n.age RETURN keys(n), properties(n)",
                    {{"[dob, hasPhD, isFrench, name]",
                      "{dob: 18/01, hasPhD: true, isFrench: true, name: Remy}"}});
}

TEST_F(KeysAndPropertiesTest, keysOfACreatedNode) {
    expectWriteRows("CREATE (n:Tag {rank: 2, name: 'x'}) RETURN keys(n), properties(n)",
                    {{"[name, rank]", "{name: x, rank: 2}"}});
}

TEST_F(KeysAndPropertiesTest, keysOfACreatedEdge) {
    expectWriteRows("CREATE (:S)-[e:E {weight: 1.5, label: 'w'}]->(:T) RETURN keys(e), properties(e)",
                    {{"[label, weight]", "{label: w, weight: 1.500000}"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAMergedNode) {
    expectWriteRows("MERGE (n:Tag {name: 'y'}) RETURN keys(n)", {{"[name]"}});
    expectWriteRows("MERGE (n:Person {name: 'Remy'}) RETURN keys(n)", {{"[age, dob, hasPhD, isFrench, name]"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAnUnwoundNode) {
    expectRows("MATCH (n:Person {name: 'Maxime'}) WITH collect(n) AS ns UNWIND ns AS m RETURN keys(m)",
               {{"[dob, hasPhD, isFrench, name]"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAnUnwoundMap) {
    expectRows("UNWIND [{b: 1}, {a: 2, c: 3}, null] AS m RETURN keys(m)",
               {{"[a, c]"}, {"[b]"}, {"null"}});
}

TEST_F(KeysAndPropertiesTest, propertiesOfAnUnwoundMap) {
    expectRows("UNWIND [{a: 1}, null] AS m RETURN properties(m)", {{"null"}, {"{a: 1}"}});
}

TEST_F(KeysAndPropertiesTest, keysOfAnUnwoundNodeOrMap) {
    expectRows("MATCH (n:Person {name: 'Martina'}) UNWIND [n, {z: 1}] AS x RETURN keys(x), properties(x)", {
        {"[hasPhD, isFrench, name]", "{hasPhD: true, isFrench: false, name: Martina}"},
        {"[z]", "{z: 1}"},
    });
}

TEST_F(KeysAndPropertiesTest, rejectsACellHoldingNoMap) {
    expectError("UNWIND [{a: 1}, 1] AS x RETURN keys(x)", "keys() reads a node, a relationship or a map");
    expectError("WITH {v: 1} AS m RETURN properties(m.v)", "properties() reads a node, a relationship or a map");
}

TEST_F(KeysAndPropertiesTest, readsAWrittenValueAsThePropertyType) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.score = 1.5");

    expectWriteRows("MATCH (n:Person {name: 'Adam'}) SET n.score = 2 RETURN properties(n).score", {{"2.000000"}});
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN properties(n).score", {{"2.000000"}});
}
