#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class KeysAfterWriteInQueryTest : public WriteQueryTest {
};

TEST_F(KeysAfterWriteInQueryTest, seesASetOnACreatedNode) {
    expectWriteRows("CREATE (n:Tag {a: 1}) SET n.a = 2, n.b = 'x' RETURN keys(n), properties(n)",
                    {{"[a, b]", "{a: 2, b: x}"}});
}

TEST_F(KeysAfterWriteInQueryTest, dropsAPropertyRemovedFromACreatedNode) {
    expectWriteRows("CREATE (n:Tag {a: 1, c: 3}) SET n.a = null RETURN keys(n), properties(n)",
                    {{"[c]", "{c: 3}"}});
}

TEST_F(KeysAfterWriteInQueryTest, seesASetOnACreatedEdge) {
    expectWriteRows("CREATE (:S)-[e:E {w: 1}]->(:T) SET e.w = 2, e.z = 'q' RETURN keys(e), properties(e)",
                    {{"[w, z]", "{w: 2, z: q}"}});
}

TEST_F(KeysAfterWriteInQueryTest, seesASetAfterAWith) {
    expectWriteRows("CREATE (n:Tag {a: 1}) WITH n SET n.b = 'x' RETURN keys(n), properties(n)",
                    {{"[a, b]", "{a: 1, b: x}"}});
}

TEST_F(KeysAfterWriteInQueryTest, readsTheKeysBeforeAndAfterASet) {
    expectWriteRows("CREATE (n:Tag {a: 1}) WITH n, keys(n) AS before SET n.b = 'x' RETURN before, keys(n)",
                    {{"[a]", "[a, b]"}});
}

TEST_F(KeysAfterWriteInQueryTest, seesASetInsideAnUnwind) {
    expectWriteRows("CREATE (n:Tag {a: 1}) WITH n UNWIND [1, 2] AS i SET n.b = i RETURN i, keys(n)",
                    {{"1", "[a, b]"}, {"2", "[a, b]"}});
}
