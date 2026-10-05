#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An OPTIONAL CALL body that creates an entity and returns it for some of its input rows:
// the other rows come out with a null entity, which names nothing the change wrote
class OptionalCallWrittenEntityTest : public WriteQueryTest {
};

TEST_F(OptionalCallWrittenEntityTest, readsANullPropertyOffAPaddedNode) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "OPTIONAL CALL (name) { CREATE (n:Person {name: name}) WITH n WHERE name = 'Nia' RETURN n } "
                    "RETURN name, n.name",
                    {{"Nia", "Nia"}, {"Remy", "null"}});
}

TEST_F(OptionalCallWrittenEntityTest, readsANullPropertyOffABodyThatReturnedNothing) {
    expectWriteRows("OPTIONAL CALL { CREATE (n:Person {name: 'Kai'}) WITH n WHERE false RETURN n } "
                    "RETURN n.name",
                    {{"null"}});
}

TEST_F(OptionalCallWrittenEntityTest, readsNullLabelsOffAPaddedNode) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "OPTIONAL CALL (name) { CREATE (n:Person {name: name}) WITH n WHERE name = 'Nia' RETURN n } "
                    "RETURN name, labels(n)",
                    {{"Nia", "[Person]"}, {"Remy", "null"}});
}

TEST_F(OptionalCallWrittenEntityTest, readsANullTypeAndPropertyOffAPaddedEdge) {
    expectWriteRows("UNWIND [1, 2] AS w "
                    "OPTIONAL CALL (w) { CREATE (:A)-[e:LINK {weight: w}]->(:B) WITH e WHERE w = 2 RETURN e } "
                    "RETURN w, type(e), e.weight",
                    {{"1", "null", "null"}, {"2", "LINK", "2"}});
}

TEST_F(OptionalCallWrittenEntityTest, readsEveryPropertyWhenTheBodyReturnsEveryRow) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "OPTIONAL CALL (name) { CREATE (n:Person {name: name}) RETURN n } "
                    "RETURN name, n.name, labels(n)",
                    {{"Nia", "Nia", "[Person]"}, {"Remy", "Remy", "[Person]"}});
}
