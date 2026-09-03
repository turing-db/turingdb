#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class ScanByPropertyValueV3Test : public CallV3Test {
protected:
    void expectSortedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ScanByPropertyValueV3Test, integerEqualityYieldsTheMatchingNodes) {
    expectSortedRows("MATCH (n) WHERE n.age = 32 RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, constantOnTheLeft) {
    expectSortedRows("MATCH (n) WHERE 32 = n.age RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, unmatchedIntegerYieldsNothing) {
    expectSortedRows("MATCH (n) WHERE n.age = 33 RETURN n.name", {});
}

TEST_F(ScanByPropertyValueV3Test, stringEquality) {
    expectSortedRows("MATCH (n) WHERE n.name = 'Remy' RETURN n", {{"0"}});
}

TEST_F(ScanByPropertyValueV3Test, unmatchedStringYieldsNothing) {
    expectSortedRows("MATCH (n) WHERE n.name = 'Nobody' RETURN n", {});
}

TEST_F(ScanByPropertyValueV3Test, boolEquality) {
    expectSortedRows("MATCH (n) WHERE n.isFrench = true RETURN n.name",
                     {{"Adam"}, {"Luc"}, {"Maxime"}, {"Remy"}});
    expectSortedRows("MATCH (n) WHERE n.isFrench = false RETURN n.name",
                     {{"Cyrus"}, {"Doruk"}, {"Martina"}, {"Suhas"}});
}

TEST_F(ScanByPropertyValueV3Test, yieldsInScanOrder) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n.isFrench = true RETURN n", sink);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}, {"8"}, {"9"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The analyzer refuses equality on doubles outright and admits every other literal only
// against a property of its own kind, so the fused scan never sees another pairing.
TEST_F(ScanByPropertyValueV3Test, literalOfAnotherKindIsRejectedBeforeTheScan) {
    const std::string_view incompatible = "Operands are not valid or compatible types";

    runQueryExpectingError("MATCH (n) WHERE n.age = 32.0 RETURN n.name", incompatible);
    runQueryExpectingError("MATCH (n) WHERE n.age = 'thirty-two' RETURN n.name", incompatible);
    runQueryExpectingError("MATCH (n) WHERE n.name = 32 RETURN n.name", incompatible);
    runQueryExpectingError("MATCH (n) WHERE n.isFrench = 1 RETURN n.name", incompatible);
}

TEST_F(ScanByPropertyValueV3Test, doubleEqualityIsRejectedBeforeTheScan) {
    runWrite("MATCH (n {name: 'Remy'}) SET n.height = 1.8");

    runQueryExpectingError("MATCH (n) WHERE n.height = 1.8 RETURN n.name", "Equality of types");
    runQueryExpectingError("MATCH (n) WHERE n.height = 2 RETURN n.name",
                           "Operands are not valid or compatible types");
}

TEST_F(ScanByPropertyValueV3Test, labelledScanKeepsOnlyTheLabelledNodes) {
    expectSortedRows("MATCH (n:Person) WHERE n.isFrench = false RETURN n.name",
                     {{"Cyrus"}, {"Doruk"}, {"Martina"}, {"Suhas"}});
    expectSortedRows("MATCH (n:Interest) WHERE n.isFrench = false RETURN n.name", {});
    expectSortedRows("MATCH (n:Founder) WHERE n.age = 32 RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, labelConjunctionNarrowsTheScan) {
    expectSortedRows("MATCH (n:Person:Founder) WHERE n.isFrench = true RETURN n.name", {{"Adam"}, {"Remy"}});
    expectSortedRows("MATCH (n:Person:SoftwareEngineering) WHERE n.isFrench = true RETURN n.name", {{"Luc"}, {"Remy"}});
    expectSortedRows("MATCH (n:Person:Bioinformatics) WHERE n.isFrench = false RETURN n.name", {{"Martina"}});
}

TEST_F(ScanByPropertyValueV3Test, labelAbsentFromTheSchemaIsRejectedBeforeTheScan) {
    runQueryExpectingError("MATCH (n:Nobody) WHERE n.age = 32 RETURN n.name", "Unknown label: Nobody");
}

TEST_F(ScanByPropertyValueV3Test, labelledScanYieldsInScanOrder) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) WHERE n.isFrench = true RETURN n", sink);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}, {"8"}, {"9"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByPropertyValueV3Test, labelledScanSeesAPropertySetOnAnEarlierNode) {
    runWrite("MATCH (n {name: 'Luc'}) SET n.age = 32");

    expectSortedRows("MATCH (n:Person) WHERE n.age = 32 RETURN n.name", {{"Adam"}, {"Luc"}, {"Remy"}});
    expectSortedRows("MATCH (n:SoftwareEngineering) WHERE n.age = 32 RETURN n.name", {{"Luc"}, {"Remy"}});
    expectSortedRows("MATCH (n:Interest) WHERE n.age = 32 RETURN n.name", {});
}

TEST_F(ScanByPropertyValueV3Test, labelledScanDropsAValueOverwrittenLater) {
    runWrite("MATCH (n {name: 'Remy'}) SET n.age = 40");

    expectSortedRows("MATCH (n:Person) WHERE n.age = 32 RETURN n.name", {{"Adam"}});
    expectSortedRows("MATCH (n:Person) WHERE n.age = 40 RETURN n.name", {{"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, labelledScanDropsADeletedNode) {
    runWrite("MATCH (n {name: 'Adam'}) DETACH DELETE n");

    expectSortedRows("MATCH (n:Person) WHERE n.age = 32 RETURN n.name", {{"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, conjunctionKeepsTheOtherPredicate) {
    expectSortedRows("MATCH (n) WHERE n.age = 32 AND n.isFrench = true RETURN n.name", {{"Adam"}, {"Remy"}});
    expectSortedRows("MATCH (n) WHERE n.age = 32 AND n.name = 'Remy' RETURN n.name", {{"Remy"}});
    expectSortedRows("MATCH (n) WHERE n.isFrench = true AND n.hasPhD = false RETURN n.name", {{"Maxime"}});
}

TEST_F(ScanByPropertyValueV3Test, disjunctionStaysAFilter) {
    expectSortedRows("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Luc' RETURN n.name", {{"Luc"}, {"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, propertyEqualityAgainstAnotherPropertyStaysAFilter) {
    expectSortedRows("MATCH (n) WHERE n.isFrench = n.hasPhD RETURN n.name",
                     {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Remy"}, {"Suhas"}});
}

TEST_F(ScanByPropertyValueV3Test, expandedRootMatchesTheNodeIDForm) {
    StringRowSink byName;
    runQuery("MATCH (n)-->(m) WHERE n.name = 'Remy' RETURN n, m", byName);

    StringRowSink byID;
    runQuery("MATCH (n)-->(m) WHERE n = 0 RETURN n, m", byID);

    std::vector<StringRowSink::Row> byNameRows;
    byName.sortedRows(byNameRows);
    std::vector<StringRowSink::Row> byIDRows;
    byID.sortedRows(byIDRows);

    EXPECT_FALSE(byNameRows.empty());
    EXPECT_EQ(byNameRows, byIDRows);
}

TEST_F(ScanByPropertyValueV3Test, propertyOfTheHopTargetStaysAFilter) {
    expectSortedRows("MATCH (n)-->(m) WHERE m.name = 'Bio' RETURN n.name", {{"Adam"}, {"Maxime"}});
}

TEST_F(ScanByPropertyValueV3Test, equalityInACrossProductFactor) {
    expectSortedRows("MATCH (n), (m) WHERE n.age = 32 RETURN count(m)", {{"36"}});
}

TEST_F(ScanByPropertyValueV3Test, multiMatchBothClausesFiltered) {
    expectSortedRows("MATCH (n) WHERE n.age = 32 MATCH (m) WHERE m.isFrench = false RETURN count(*)", {{"8"}});
}

TEST_F(ScanByPropertyValueV3Test, multiMatchFilteredClauseExpandedByALaterClause) {
    expectSortedRows("MATCH (n) WHERE n.name = 'Adam' MATCH (n)-->(m) RETURN m.name",
                     {{"Bio"}, {"Cooking"}, {"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, countsOverTheMatchingSet) {
    expectSortedRows("MATCH (n) WHERE n.isFrench = true RETURN count(n)", {{"4"}});
}

TEST_F(ScanByPropertyValueV3Test, limitBoundsTheScan) {
    StringRowSink sink;
    runQuery("MATCH (n) WHERE n.isFrench = true RETURN n LIMIT 2", sink);

    const std::vector<StringRowSink::Row> expected {{"0"}, {"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ScanByPropertyValueV3Test, returnsOtherPropertiesOfTheMatchingNodes) {
    expectSortedRows("MATCH (n) WHERE n.age = 32 RETURN n.name, n.dob", {{"Adam", "18/08"}, {"Remy", "18/01"}});
}

TEST_F(ScanByPropertyValueV3Test, aValueOverwrittenInALaterCommitNoLongerMatches) {
    runWrite("MATCH (n {name: 'Remy'}) SET n.age = 40");

    expectSortedRows("MATCH (n) WHERE n.age = 32 RETURN n.name", {{"Adam"}});
    expectSortedRows("MATCH (n) WHERE n.age = 40 RETURN n.name", {{"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, aValueRewrittenToItselfMatchesOnce) {
    runWrite("MATCH (n {name: 'Adam'}) SET n.age = 32");

    expectSortedRows("MATCH (n) WHERE n.age = 32 RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, aValueOverwrittenTwiceMatchesOnlyTheLatest) {
    runWrite("MATCH (n {name: 'Remy'}) SET n.age = 40");
    runWrite("MATCH (n {name: 'Remy'}) SET n.age = 41");

    expectSortedRows("MATCH (n) WHERE n.age = 32 RETURN n.name", {{"Adam"}});
    expectSortedRows("MATCH (n) WHERE n.age = 40 RETURN n.name", {});
    expectSortedRows("MATCH (n) WHERE n.age = 41 RETURN n.name", {{"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, aPropertySetOnANodeCreatedEarlierMatches) {
    runWrite("MATCH (n {name: 'Luc'}) SET n.age = 32");

    expectSortedRows("MATCH (n) WHERE n.age = 32 RETURN n.name", {{"Adam"}, {"Luc"}, {"Remy"}});
}

TEST_F(ScanByPropertyValueV3Test, aDeletedNodeNoLongerMatches) {
    runWrite("MATCH (n {name: 'Adam'}) DETACH DELETE n");

    expectSortedRows("MATCH (n) WHERE n.age = 32 RETURN n.name", {{"Remy"}});
    expectSortedRows("MATCH (n) WHERE n.isFrench = true RETURN n.name", {{"Luc"}, {"Maxime"}, {"Remy"}});
}
