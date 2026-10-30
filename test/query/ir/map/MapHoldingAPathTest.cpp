#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A map value may hold a path, and what is read out of it is that path. Remy KNOWS_WELL Adam
// over edge 0, so the one path below is (0), [0], (1).
class MapHoldingAPathTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

// Disabled: lowering cannot read a path column into a map (TUR-153).
TEST_F(MapHoldingAPathTest, DISABLED_returnsThePathTheMapHolds) {
    expectRows("MATCH p=(:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) WITH {path: p} AS m RETURN m.path",
               {{"(0), [0], (1)"}});
}

TEST_F(MapHoldingAPathTest, DISABLED_readsTheLengthOfThePathTheMapHolds) {
    expectRows("MATCH p=(:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) WITH {path: p} AS m RETURN length(m.path)",
               {{"1"}});
}

TEST_F(MapHoldingAPathTest, DISABLED_readsTheLengthOfThePathAListedMapHolds) {
    expectRows("MATCH p=(:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) WITH [{path: p}] AS l RETURN length(l[0].path)",
               {{"1"}});
}
