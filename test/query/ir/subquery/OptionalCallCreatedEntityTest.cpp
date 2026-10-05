#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An OPTIONAL CALL returning an entity its body created: the row it pads names no entity,
// so none this change wrote
class OptionalCallCreatedEntityTest : public WriteQueryTest {
};

TEST_F(OptionalCallCreatedEntityTest, readsAPropertyOfAPaddedCreatedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Kai'] AS name "
                    "OPTIONAL CALL (name) { CREATE (n:Person {name: name}) WITH n WHERE name = 'Kai' RETURN n } "
                    "RETURN name, n.name, n:Person",
                    {{"Kai", "Kai", "true"}, {"Remy", "null", "false"}});
}

TEST_F(OptionalCallCreatedEntityTest, setsAPropertyOfAPaddedCreatedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Kai'] AS name "
                    "OPTIONAL CALL (name) { CREATE (n:Person {name: name}) WITH n WHERE name = 'Kai' RETURN n } "
                    "SET n.age = 7 RETURN name, n.age",
                    {{"Kai", "7"}, {"Remy", "null"}});

    expectRows("MATCH (p:Person {name: 'Kai'}) RETURN p.age", {{"7"}});
}

TEST_F(OptionalCallCreatedEntityTest, padsAWhenBranchThatCreated) {
    expectWriteRows("UNWIND ['Remy', 'Kai'] AS name "
                    "OPTIONAL CALL (name) { WHEN name = 'Kai' THEN { CREATE (n:Person {name: name}) RETURN n } } "
                    "SET n.age = 7 RETURN name, n.name, n.age",
                    {{"Kai", "Kai", "7"}, {"Remy", "null", "null"}});
}

TEST_F(OptionalCallCreatedEntityTest, padsAUnionWhoseBranchesCreated) {
    expectWriteRows("UNWIND ['Remy', 'Kai'] AS name "
                    "OPTIONAL CALL (name) { "
                    "CREATE (n:Person {name: name}) WITH n WHERE name = 'Kai' RETURN n "
                    "UNION "
                    "CREATE (n:Robot {name: name}) WITH n WHERE name = 'Kai' RETURN n "
                    "} "
                    "RETURN name, n.name, n:Robot",
                    {{"Kai", "Kai", "false"}, {"Kai", "Kai", "true"}, {"Remy", "null", "false"}});
}

TEST_F(OptionalCallCreatedEntityTest, padsAnImportedCreatedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Kai'] AS name CREATE (k:Person {name: name + '2'}) "
                    "WITH name, k "
                    "OPTIONAL CALL (name, k) { WITH k WHERE name = 'Kai' RETURN k AS n } "
                    "RETURN name, n.name",
                    {{"Kai", "Kai2"}, {"Remy", "null"}});
}
