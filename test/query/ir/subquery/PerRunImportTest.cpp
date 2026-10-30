#include <gtest/gtest.h>

#include <fstream>
#include <string>
#include <string_view>

#include "QueryStatus.h"
#include "versioning/ChangeID.h"

#include "TuringTestEnv.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// What the scope clause of a CALL, an EXISTS, a COUNT or a COLLECT imports holds one value
// for a body run once per row, so it stays readable past every barrier of that body: a
// keyless aggregate, a NEXT segment, a segment that writes. simpledb's people have 15
// INTERESTED_IN edges, Remy 3 of them, and Remy and Adam know each other well.
class PerRunImportTest : public WriteQueryTest {
protected:
    void writeFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
    }

    void expectWriteError(std::string_view query, std::string_view expectedError) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(PerRunImportTest, readsTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c RETURN p.name AS name, c } "
               "RETURN name, c",
               {{"Adam", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"},
                {"Martina", "1"}, {"Maxime", "2"}, {"Remy", "3"}, {"Suhas", "2"}});
}

TEST_F(PerRunImportTest, readsTheImportPastAKeylessSegmentReturn) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS c NEXT RETURN p.name AS name, c } "
               "RETURN name, c",
               {{"Adam", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"},
                {"Martina", "1"}, {"Maxime", "2"}, {"Remy", "3"}, {"Suhas", "2"}});
}

TEST_F(PerRunImportTest, readsTheImportPastAKeylessWithInEachUnionBranch) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c RETURN p.name AS name, c "
               "UNION ALL MATCH (p)-[:KNOWS_WELL]->(f) WITH count(f) AS c RETURN p.name AS name, c } "
               "RETURN name, c",
               {{"Remy", "1"}, {"Remy", "3"}});
}

TEST_F(PerRunImportTest, readsTheImportPastAKeylessWithAfterADistinctOne) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH DISTINCT i.name AS n WITH count(n) AS c "
               "RETURN p.name AS name, c } "
               "RETURN name, c",
               {{"Remy", "3"}});
}

// Before the fix the MATCH declared a p of its own, which every KNOWS_WELL edge matched
TEST_F(PerRunImportTest, hopsFromTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN c, f.name AS friend } "
               "RETURN p.name, c, friend",
               {{"Adam", "2", "Remy"}, {"Remy", "3", "Adam"}});
}

TEST_F(PerRunImportTest, correlatesAnExistsWithTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c "
               "WHERE EXISTS { (p)-[:KNOWS_WELL]->() } RETURN c } "
               "RETURN p.name, c",
               {{"Adam", "2"}, {"Remy", "3"}});
}

TEST_F(PerRunImportTest, projectsTheImportThroughAWildcardPastAKeylessWith) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c WITH * "
               "RETURN p.name AS name, c } "
               "RETURN name, c",
               {{"Remy", "3"}});
}

TEST_F(PerRunImportTest, returnsTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c RETURN p AS q, c } "
               "RETURN q.name, c",
               {{"Adam", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"},
                {"Martina", "1"}, {"Maxime", "2"}, {"Remy", "3"}, {"Suhas", "2"}});
}

TEST_F(PerRunImportTest, collectsTheImportOverTheRowsPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c UNWIND range(1, c) AS k "
               "RETURN size(collect(p)) AS n } "
               "RETURN p.name, n",
               {{"Adam", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"},
                {"Martina", "1"}, {"Maxime", "2"}, {"Remy", "3"}, {"Suhas", "2"}});
}

// The inner body runs once per row of the outer one, and reads the outer import beside its own
TEST_F(PerRunImportTest, nestsABodyReadingTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c "
               "CALL (p, c) { MATCH (p)-[:KNOWS_WELL]->(f) WITH count(f) AS k RETURN p.name AS name, c + k AS total } "
               "RETURN name, total } "
               "RETURN name, total",
               {{"Adam", "3"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"},
                {"Martina", "1"}, {"Maxime", "2"}, {"Remy", "4"}, {"Suhas", "2"}});
}

TEST_F(PerRunImportTest, readsTheImportPastASortedAndCutWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH i ORDER BY i.name LIMIT 1 "
               "RETURN p.name AS name, i.name AS first } "
               "RETURN name, first",
               {{"Adam", "Bio"}, {"Cyrus", "Gym"}, {"Doruk", "Gym"}, {"Luc", "Animals"},
                {"Martina", "Cooking"}, {"Maxime", "Bio"}, {"Remy", "Computers"}, {"Suhas", "Gym"}});
}

TEST_F(PerRunImportTest, countsTheRowsOfABodyReadingTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name AS name, "
               "COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c WHERE c > 1 RETURN p.name AS n } AS k",
               {{"Adam", "1"}, {"Cyrus", "1"}, {"Doruk", "0"}, {"Luc", "1"},
                {"Martina", "0"}, {"Maxime", "1"}, {"Remy", "1"}, {"Suhas", "1"}});
}

TEST_F(PerRunImportTest, findsTheRowsOfABodyReadingTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name AS name, "
               "EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c WHERE c > 2 RETURN p } AS found",
               {{"Adam", "false"}, {"Cyrus", "false"}, {"Doruk", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Maxime", "false"}, {"Remy", "true"}, {"Suhas", "false"}});
}

TEST_F(PerRunImportTest, collectsTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c "
               "RETURN p.name + ':' + toString(c) } AS tally",
               {{"[Adam:2]"}, {"[Cyrus:2]"}, {"[Doruk:1]"}, {"[Luc:2]"},
                {"[Martina:1]"}, {"[Maxime:2]"}, {"[Remy:3]"}, {"[Suhas:2]"}});
}

TEST_F(PerRunImportTest, writesTheImportPastAKeylessWith) {
    applyWrite("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c SET p.interests = c }");

    expectRows("MATCH (p:Person) RETURN p.name, p.interests",
               {{"Adam", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"},
                {"Martina", "1"}, {"Maxime", "2"}, {"Remy", "3"}, {"Suhas", "2"}});
}

TEST_F(PerRunImportTest, readsTheImportAfterAWritingSegmentPastAKeylessReturn) {
    expectWriteRows("MATCH (p:Person) WHERE p.name = 'Remy' "
                    "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS c "
                    "NEXT CREATE (:Tally {c: c}) NEXT RETURN p.name AS name } "
                    "RETURN name",
                    {{"Remy"}});

    expectRows("MATCH (t:Tally) RETURN t.c", {{"3"}});
}

TEST_F(PerRunImportTest, readsAMergedImportPastAKeylessWith) {
    expectWriteRows("MERGE (n:Fresh {name: 'x'}) "
                    "CALL (n) { UNWIND [1, 2] AS k WITH count(k) AS c RETURN n.name AS name, c } "
                    "RETURN name, c",
                    {{"x", "2"}});
}

TEST_F(PerRunImportTest, readsACreatedImportPastAKeylessWith) {
    expectWriteRows("CREATE (n:Fresh {name: 'y'}) "
                    "CALL (n) { UNWIND [1, 2] AS k WITH count(k) AS c RETURN n.name AS name, c } "
                    "RETURN name, c",
                    {{"y", "2"}});
}

TEST_F(PerRunImportTest, readsAMergedImportAfterAWritingSegment) {
    expectWriteRows("MERGE (n:Fresh {name: 'z'}) "
                    "CALL (n) { UNWIND [1] AS k RETURN k NEXT CREATE (:Tally) NEXT RETURN n.name AS name } "
                    "RETURN name",
                    {{"z"}});
}

// What the write ahead of the body knows of an import holds past a segment that writes, so a
// read the engine turns away everywhere else is turned away there too
TEST_F(PerRunImportTest, rejectsAnOptionalMatchOverACreatedImportPastAWritingSegment) {
    expectWriteError("CREATE (n:Fresh {name: 'm'})-[:R]->(:Other) WITH n "
                     "CALL (n) { UNWIND [1] AS k RETURN k NEXT CREATE (:T) "
                     "NEXT OPTIONAL MATCH (n)-[:R]->(o) RETURN o IS NULL AS missing } "
                     "RETURN missing",
                     "An OPTIONAL MATCH cannot read what a CREATE in the same query wrote: 'n'");
}

TEST_F(PerRunImportTest, rejectsAnOptionalMatchOverAMergedImportPastAWritingSegment) {
    expectWriteError("MERGE (n:Fresh {name: 'q'}) WITH n "
                     "CALL (n) { UNWIND [1] AS k RETURN k NEXT CREATE (:T) "
                     "NEXT OPTIONAL MATCH (n)-[:R]->(o) RETURN o IS NULL AS missing } "
                     "RETURN missing",
                     "An OPTIONAL MATCH cannot read what a MERGE in the same query wrote: 'n'");
}

TEST_F(PerRunImportTest, rejectsAPatternComprehensionOverACreatedImportPastAWritingSegment) {
    expectWriteError("CREATE (n:Fresh {name: 'm'})-[:R]->(:Other) WITH n "
                     "CALL (n) { UNWIND [1] AS k RETURN k NEXT CREATE (:T) "
                     "NEXT RETURN [(n)-[:R]->(o) | o] AS os } "
                     "RETURN size(os)",
                     "A pattern comprehension cannot read what a CREATE in the same query wrote: 'n'");
}

TEST_F(PerRunImportTest, readsTheCSVRowAfterTheCallOfAWritingSegment) {
    writeFile("people.csv", "Ada,36\nGrace,45\n");

    expectWriteRows("LOAD CSV 'people.csv' AS row "
                    "CALL (row) { UNWIND [1] AS x RETURN x AS x NEXT CREATE (:T) NEXT RETURN 1 AS one } "
                    "RETURN one, row[0] AS name",
                    {{"1", "Ada"}, {"1", "Grace"}});
}

TEST_F(PerRunImportTest, readsTheCSVRowPastAWritingSegment) {
    writeFile("people.csv", "Ada,36\nGrace,45\n");

    expectWriteRows("LOAD CSV 'people.csv' AS row "
                    "CALL (row) { UNWIND [1] AS x RETURN x AS x NEXT CREATE (:T) NEXT RETURN row[0] AS name } "
                    "RETURN name",
                    {{"Ada"}, {"Grace"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
