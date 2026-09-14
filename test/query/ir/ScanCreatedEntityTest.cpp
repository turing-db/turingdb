#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "iterators/ChunkConfig.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class ScanCreatedEntityTest : public CallV3Test {
};

// A scan below the cut reads the nodes the CREATE above it wrote: the graph holds none of
// them until the commit, so the scan reads them out of the write buffer once it has walked
// the graph's own.
TEST_F(ScanCreatedEntityTest, scansANodeCreatedAboveTheCut) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Ana'}) WITH n MATCH (m) RETURN count(m)", sink);

    const std::vector<StringRowSink::Row> expected {{"19"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanCreatedEntityTest, scansACreatedNodeByLabel) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Ana'}) WITH n MATCH (m:Person) RETURN count(m)", sink);

    const std::vector<StringRowSink::Row> expected {{"9"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The label is the change's own: the graph's schema does not have it, so the scan resolves
// the name against what the change wrote and matches the pending node with it.
TEST_F(ScanCreatedEntityTest, scansACreatedNodeByALabelTheChangeIntroduced) {
    StringRowSink sink;
    runWrite("CREATE (n:Brand {name: 'Ora'}) WITH n MATCH (m:Brand) RETURN m.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Ora"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A label the created node does not carry leaves the scan with the graph's own rows.
TEST_F(ScanCreatedEntityTest, scansNoCreatedNodeOfAnotherLabel) {
    StringRowSink sink;
    runWrite("CREATE (n:Brand {name: 'Ora'}) WITH n MATCH (m:Sales) RETURN m.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Doruk"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanCreatedEntityTest, scansACreatedNodeByPropertyValue) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Ana'}) WITH n MATCH (m {name: 'Ana'}) RETURN m.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Ana"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanCreatedEntityTest, scansACreatedNodeByPropertyValueAndLabel) {
    StringRowSink matching;
    runWrite("CREATE (n:Person {name: 'Ana'}) WITH n MATCH (m:Person {name: 'Ana'}) RETURN m.name", matching);

    const std::vector<StringRowSink::Row> expected {{"Ana"}};
    EXPECT_EQ(matching.getRows(), expected);

    StringRowSink other;
    runWrite("CREATE (n:Person {name: 'Bea'}) WITH n MATCH (m:Interest {name: 'Bea'}) RETURN m.name", other);
    EXPECT_TRUE(other.getRows().empty());
}

// The property name is the change's own, so the scan resolves it the way it resolves a
// label the change introduced.
TEST_F(ScanCreatedEntityTest, scansACreatedNodeByAPropertyTheChangeIntroduced) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {nickname: 'Nan'}) WITH n MATCH (m {nickname: 'Nan'}) RETURN m.nickname", sink);

    const std::vector<StringRowSink::Row> expected {{"Nan"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanCreatedEntityTest, scansNoCreatedNodeOfAnotherValue) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Ana'}) WITH n MATCH (m {name: 'Zoe'}) RETURN m.name", sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// The scan reads the properties of what it matched out of the write buffer too.
TEST_F(ScanCreatedEntityTest, readsThePropertiesOfACreatedNodeItScanned) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Ana', age: 41}) WITH n MATCH (m:Person {name: 'Ana'}) RETURN m.name, m.age",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Ana", "41"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The change wrote the node and dropped it again, so the scan below the cut has nothing of
// it to read.
TEST_F(ScanCreatedEntityTest, scansNoNodeTheChangeDeleted) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'}), (b:Person {name: 'Bo'}) WITH a, b DELETE b WITH a MATCH (m:Person {name: 'Bo'}) RETURN m.name",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(ScanCreatedEntityTest, scansACreatedEdge) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH a MATCH (x)-->(y) RETURN count(x)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"19"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanCreatedEntityTest, scansACreatedEdgeByType) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:MENTORS]->(b:Person {name: 'Bo'}) WITH a MATCH (x)-[:MENTORS]->(y) RETURN x.name, y.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Ana", "Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A type the created edge does not carry leaves the scan with the graph's own rows.
TEST_F(ScanCreatedEntityTest, scansNoCreatedEdgeOfAnotherType) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:MENTORS]->(b:Person {name: 'Bo'}) WITH a MATCH (x)-[:KNOWS_WELL]->(y) RETURN count(x)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// More pending nodes than one chunk holds, so the scan fills several: what it reports is
// what the commit holds.
TEST_F(ScanCreatedEntityTest, scansMoreCreatedNodesThanAChunkHolds) {
    StringRowSink scanned;
    runWrite("MATCH (a), (b), (c), (d) CREATE (x:Bulk) WITH count(x) AS made MATCH (m:Bulk) RETURN count(m)", scanned);

    ASSERT_EQ(scanned.getRows().size(), 1u);
    EXPECT_GT(std::stoull(scanned.getRows().front().front()), ChunkConfig::CHUNK_SIZE);

    StringRowSink committed;
    runQuery("MATCH (m:Bulk) RETURN count(m)", committed);
    EXPECT_EQ(scanned.getRows(), committed.getRows());
}

// The write of an earlier statement of the same change is staged, not committed, and a scan
// reads the change's tip: it sees that write once the change commits it, as it sees any
// other.
TEST_F(ScanCreatedEntityTest, scansNothingAnEarlierStatementStaged) {
    StringRowSink nodes;
    runWritesInOneChange("CREATE (n:Brand {name: 'Ora'})", "MATCH (m:Brand) RETURN m.name", nodes);
    EXPECT_TRUE(nodes.getRows().empty());

    StringRowSink edges;
    runWritesInOneChange("CREATE (a:Person {name: 'Ana'})-[:MENTORS]->(b:Person {name: 'Bo'})",
                         "MATCH (x)-[:MENTORS]->(y) RETURN y.name",
                         edges);
    EXPECT_TRUE(edges.getRows().empty());
}

// The searched value is compared as the property's own type, so a scan matches a created
// node on a number or a boolean as it does on a string.
TEST_F(ScanCreatedEntityTest, scansACreatedNodeByANumericAndABooleanValue) {
    StringRowSink numeric;
    runWrite("CREATE (n:Person {name: 'Ana', age: 32}) WITH n MATCH (m {age: 32}) RETURN count(m)", numeric);

    const std::vector<StringRowSink::Row> counted {{"3"}};
    EXPECT_EQ(numeric.getRows(), counted);

    StringRowSink boolean;
    runWrite("CREATE (n:Person {name: 'Bea', isFrench: true}) WITH n MATCH (m {isFrench: true}) RETURN count(m)",
             boolean);

    const std::vector<StringRowSink::Row> flagged {{"5"}};
    EXPECT_EQ(boolean.getRows(), flagged);
}
