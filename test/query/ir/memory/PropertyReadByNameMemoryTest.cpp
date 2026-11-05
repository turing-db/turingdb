#include <gtest/gtest.h>

#include <stddef.h>

#include <string_view>

#include "CallV3Test.h"
#include "HeapSamplingSink.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t MEGABYTE = 1024 * 1024;
constexpr size_t ROW_COUNT = 1000000;

}

// Each query reads one property by name, a million times, off a node or an edge that also
// holds a list of 100 integers the same query wrote. A read copies only the property it
// names, so the heap holds one chunk of columns, not a copy of the list per row.
class PropertyReadByNameMemoryTest : public CallV3Test {
protected:
    void expectBoundedGrowth(std::string_view query) {
        HeapSamplingSink sink;
        runWrite(query, sink);

        const size_t growth = sink.getGrowthSinceStart();

        EXPECT_EQ(sink.getRowCount(), ROW_COUNT) << query;
        EXPECT_LT(growth, 32 * MEGABYTE) << query << "\ngrew by " << growth / MEGABYTE << " MB";
    }
};

TEST_F(PropertyReadByNameMemoryTest, readsAPropertyOfACreatedNode) {
    expectBoundedGrowth("CREATE (n:T {v: [1, 'a'][0], big: range(1, 100)}) "
                        "WITH n UNWIND range(1, 1000000) AS i RETURN n.v");
}

TEST_F(PropertyReadByNameMemoryTest, readsAPropertyOfACreatedEdge) {
    expectBoundedGrowth("CREATE (:T)-[e:R {w: [1, 'a'][0], big: range(1, 100)}]->(:T) "
                        "WITH e UNWIND range(1, 1000000) AS i RETURN e.w");
}

TEST_F(PropertyReadByNameMemoryTest, readsAPropertyOfAnUpdatedNodeACellHolds) {
    expectBoundedGrowth("MATCH (n:Person {name: 'Remy'}) SET n.big = range(1, 100) "
                        "WITH n UNWIND range(1, 1000000) AS i RETURN [n, 1][0].name");
}

TEST_F(PropertyReadByNameMemoryTest, readsAPropertyOfAnUpdatedEdgeACellHolds) {
    expectBoundedGrowth("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
                        "SET e.big = range(1, 100) "
                        "WITH e UNWIND range(1, 1000000) AS i RETURN [e, 1][0].name");
}

TEST_F(PropertyReadByNameMemoryTest, readsAPropertyByAStringKey) {
    expectBoundedGrowth("MATCH (n:Person {name: 'Remy'}) SET n.big = range(1, 100) "
                        "WITH [n, 1] AS l, 'name' AS k UNWIND range(1, 1000000) AS i RETURN l[0][k]");
}

TEST_F(PropertyReadByNameMemoryTest, readsAPropertyByAKeyACellHolds) {
    expectBoundedGrowth("MATCH (n:Person {name: 'Remy'}) SET n.big = range(1, 100) "
                        "WITH [n, 1] AS l, ['name', 1][0] AS k UNWIND range(1, 1000000) AS i RETURN l[0][k]");
}
