#include <gtest/gtest.h>

#include <stdint.h>

#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The counts the metadata rewrite answers, run end to end over the shared SimpleGraph
// fixture: 8 Person nodes and 10 Interest ones, 18 in all, and among them 18 name, 8 hasPhD,
// 4 dob and 2 age. The products are the row counts the cross products they replace would
// have walked - MATCH (a:Person), (b:Person), (c:Person) RETURN count(*) is 512 rows the
// engine no longer builds.
class CountFromMetadataQueryTest : public WriteQueryTest {
protected:
    // A count collapses to one row, and it stays the ui64 column the shared count sink
    // reads whether the engine walked the rows or read the graph's counts.
    void expectCount(std::string_view query, uint64_t expected) {
        expectCounts(query, Counts {expected});
    }
};

TEST_F(CountFromMetadataQueryTest, countsTheNodesCarryingALabel) {
    expectCount("MATCH (a:Person) RETURN count(*)", 8);
    expectCount("MATCH (a:Interest) RETURN count(*)", 10);
}

TEST_F(CountFromMetadataQueryTest, countsEveryNodeOfAnUnlabelledScan) {
    expectCount("MATCH (a) RETURN count(*)", 18);
}

TEST_F(CountFromMetadataQueryTest, countsTheNodesCarryingEveryLabelOfAConjunction) {
    expectCount("MATCH (a:Person:Founder) RETURN count(*)", 2);
    expectCount("MATCH (a:Person:SoftwareEngineering) RETURN count(*)", 4);
}

TEST_F(CountFromMetadataQueryTest, countsTheScannedNodesThemselves) {
    expectCount("MATCH (a:Person) RETURN count(a)", 8);
}

TEST_F(CountFromMetadataQueryTest, countsTheProductOfTwoScans) {
    expectCount("MATCH (a:Person), (b:Person) RETURN count(*)", 64);
    expectCount("MATCH (a:Person), (b:Interest) RETURN count(*)", 80);
    expectCount("MATCH (a:Person), (b) RETURN count(*)", 144);
}

TEST_F(CountFromMetadataQueryTest, countsTheProductOfThreeScans) {
    expectCount("MATCH (a:Person), (b:Person), (c:Person) RETURN count(*)", 512);
    expectCount("MATCH (a:Person), (b:Interest), (c) RETURN count(*)", 1440);
}

// A label the graph never had leaves the conjunction unsatisfiable, so its scan matches no
// node and every product it stands in counts 0.
TEST_F(CountFromMetadataQueryTest, countsNothingForAnAbsentLabel) {
    expectCount("MATCH (a:Ghost) RETURN count(*)", 0);
    expectCount("MATCH (a:Person), (b:Ghost) RETURN count(*)", 0);
    expectCount("MATCH (a:Person:Ghost) RETURN count(*)", 0);
}

// count(a.p) counts the rows where the property is there, so the answer is how many of the
// scanned nodes hold it, not how many there are. The fixture's properties are sparse in
// both directions: every node has a name, only the Persons have hasPhD, only the Interests
// isReal, and age reaches two of the eight Persons.
TEST_F(CountFromMetadataQueryTest, countsTheScannedNodesHoldingAProperty) {
    expectCount("MATCH (a) RETURN count(a.name)", 18);
    expectCount("MATCH (a:Person) RETURN count(a.name)", 8);
    expectCount("MATCH (a:Person) RETURN count(a.hasPhD)", 8);
    expectCount("MATCH (a:Person) RETURN count(a.dob)", 4);
    expectCount("MATCH (a:Person) RETURN count(a.age)", 2);
    expectCount("MATCH (a:Person:Founder) RETURN count(a.age)", 2);
}

// A property none of the scanned nodes hold counts 0, whether another label holds it or the
// graph never had it at all.
TEST_F(CountFromMetadataQueryTest, countsNothingForAPropertyTheScannedNodesLack) {
    expectCount("MATCH (a:Person) RETURN count(a.isReal)", 0);
    expectCount("MATCH (a:Interest) RETURN count(a.isReal)", 7);
    expectCount("MATCH (a) RETURN count(a.isReal)", 7);
    expectCount("MATCH (a:Ghost) RETURN count(a.name)", 0);
}

// Each write lands in a data part of its own, so a node written twice holds its property in
// two of them and a deleted node keeps its entries behind a tombstone. The tally is over the
// nodes, so neither may show up in it twice or at all.
TEST_F(CountFromMetadataQueryTest, countsEachNodeOnceAcrossWrites) {
    expectCount("MATCH (a:Person) RETURN count(a.age)", 2);

    applyWrite("MATCH (a:Person {name: 'Remy'}) SET a.age = 33");
    expectCount("MATCH (a:Person) RETURN count(a.age)", 2);

    applyWrite("MATCH (a:Person {name: 'Luc'}) SET a.age = 40");
    expectCount("MATCH (a:Person) RETURN count(a.age)", 3);

    applyWrite("CREATE (a:Person {name: 'Temp', age: 1})");
    expectCount("MATCH (a:Person) RETURN count(*)", 9);
    expectCount("MATCH (a:Person) RETURN count(a.age)", 4);

    applyWrite("MATCH (a:Person {name: 'Temp'}) DELETE a");
    expectCount("MATCH (a:Person) RETURN count(*)", 8);
    expectCount("MATCH (a:Person) RETURN count(a.age)", 3);
}

// The shapes the rewrite leaves alone still answer as they did: a filtered scan, a budgeted
// prefix, a hop, a property read off a hop, and a tally that charges each node once.
TEST_F(CountFromMetadataQueryTest, countsTheShapesTheRewriteLeavesAlone) {
    expectCount("MATCH (a:Person) WHERE a.name = 'Remy' RETURN count(*)", 1);
    expectCount("MATCH (a:Person) WITH a LIMIT 3 RETURN count(*)", 3);
    expectCount("MATCH (a:Person)-[:INTERESTED_IN]->(b) RETURN count(*)", 15);
    expectCount("MATCH (a:Person)-[:INTERESTED_IN]->(b) RETURN count(b.name)", 15);
    expectCount("MATCH (a:Person), (b:Interest) RETURN count(DISTINCT a)", 8);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
