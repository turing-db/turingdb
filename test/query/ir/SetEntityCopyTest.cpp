#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET n = m leaves n holding m's properties and nothing else; SET n += m adds m's to n's.
// Remy is 32 in simpledb.
class SetEntityCopyTest : public WriteQueryTest {
};

TEST_F(SetEntityCopyTest, replacesThePropertiesOfOneNodeWithAnother) {
    expectWriteRows("CREATE (a:X {k: 1, v: 'a'}), (b:X {k: 2, w: 'b'}) SET b = a RETURN b.k, b.v, b.w",
                    {{"1", "a", "null"}});
}

TEST_F(SetEntityCopyTest, addsThePropertiesOfOneNodeToAnother) {
    expectWriteRows("CREATE (a:X {k: 1, v: 'a'}), (b:X {k: 2, w: 'b'}) SET b += a RETURN b.k, b.v, b.w",
                    {{"1", "a", "b"}});
}

TEST_F(SetEntityCopyTest, copiesACommittedNode) {
    applyWrite("MATCH (a:Person {name: 'Remy'}) CREATE (b:Clone) SET b = a");

    expectRows("MATCH (b:Clone) RETURN b.name, b.age", {{"Remy", "32"}});
}

TEST_F(SetEntityCopyTest, addsThePropertiesOfANodeBesideListAndDateTimeProperties) {
    applyWrite("CREATE (:Other {tags: [1, 2], born: datetime('2020-01-02T03:04:05Z')})");

    expectWriteRows("MATCH (l:Person {name: 'Luc'}), (r:Person {name: 'Remy'}) SET l += r RETURN l.name, l.age",
                    {{"Remy", "32"}});
}

TEST_F(SetEntityCopyTest, addsAListProperty) {
    expectWriteRows("CREATE (a:X {tags: [1, 2]}), (b:X {k: 1}) SET b += a RETURN size(b.tags), b.k", {{"2", "1"}});
}

TEST_F(SetEntityCopyTest, copiesTheNodeAnExpressionEvaluatesTo) {
    expectWriteRows("CREATE (a:X {k: 1, v: 'a'}), (b:X {k: 2, w: 'b'}) SET b = coalesce(a, b) RETURN b.k, b.v, b.w",
                    {{"1", "a", "null"}});
}

TEST_F(SetEntityCopyTest, addsTheNodeACaseEvaluatesTo) {
    expectWriteRows("CREATE (a:X {k: 1, v: 'a'}), (b:X {k: 2, w: 'b'}) SET b += CASE WHEN true THEN a END RETURN b.k, b.v, b.w",
                    {{"1", "a", "b"}});
}

TEST_F(SetEntityCopyTest, addsNothingFromAnExpressionEvaluatingToNull) {
    expectWriteRows("CREATE (a:X {k: 1, v: 'a'}), (b:X {k: 2, w: 'b'}) SET b += CASE WHEN false THEN a END RETURN b.k, b.v, b.w",
                    {{"2", "null", "b"}});
}

TEST_F(SetEntityCopyTest, copiesTheCommittedNodeAnExpressionEvaluatesTo) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b {name: 'Padel'}) SET b = coalesce(a, b)");

    expectRows("MATCH (n {name: 'Remy'}) RETURN n.age", {{"32"}, {"32"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
