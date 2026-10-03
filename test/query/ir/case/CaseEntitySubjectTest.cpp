#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A CASE whose subject is a node or an edge rather than a scalar. An OPTIONAL MATCH leaves
// the variables of the pattern it missed null, and the selection over one of them is how a
// query answers on the rows it matched and the rows it did not.
class CaseEntitySubjectTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

// Remy and Adam know each other well; the other six people have no KNOWS_WELL edge, so the
// OPTIONAL MATCH leaves their r null and the branch testing for one takes those rows
TEST_F(CaseEntitySubjectTest, matchesTheUnmatchedEdgeOnANullTest) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
               "RETURN p.name, CASE r WHEN IS NULL THEN 'none' ELSE 'some' END",
               {
                   {"Adam", "some"}, {"Cyrus", "none"}, {"Doruk", "none"}, {"Luc", "none"},
                   {"Martina", "none"}, {"Maxime", "none"}, {"Remy", "some"}, {"Suhas", "none"},
               });
}

TEST_F(CaseEntitySubjectTest, matchesTheMatchedEdgeOnANotNullTest) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
               "RETURN p.name, CASE r WHEN IS NOT NULL THEN 'some' ELSE 'none' END",
               {
                   {"Adam", "some"}, {"Cyrus", "none"}, {"Doruk", "none"}, {"Luc", "none"},
                   {"Martina", "none"}, {"Maxime", "none"}, {"Remy", "some"}, {"Suhas", "none"},
               });
}

// The node the pattern binds is null on the same six rows the edge is
TEST_F(CaseEntitySubjectTest, matchesTheUnmatchedNodeOnANullTest) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, CASE f WHEN IS NULL THEN 'nobody' ELSE 'somebody' END",
               {
                   {"Adam", "somebody"}, {"Cyrus", "nobody"}, {"Doruk", "nobody"},
                   {"Luc", "nobody"}, {"Martina", "nobody"}, {"Maxime", "nobody"},
                   {"Remy", "somebody"}, {"Suhas", "nobody"},
               });
}

// Null is equal to nothing, itself included, so the simple form never takes a WHEN null
// branch: all eight rows fall to the ELSE, the six null ones with them
TEST_F(CaseEntitySubjectTest, neverMatchesTheNullLiteral) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
               "RETURN p.name, CASE r WHEN null THEN 'none' ELSE 'some' END",
               {
                   {"Adam", "some"}, {"Cyrus", "some"}, {"Doruk", "some"}, {"Luc", "some"},
                   {"Martina", "some"}, {"Maxime", "some"}, {"Remy", "some"}, {"Suhas", "some"},
               });
}

// A null test sits beside the other values of its branch, as it does over a scalar subject
TEST_F(CaseEntitySubjectTest, mixesANullTestIntoAValueList) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, CASE f WHEN IS NULL, = 0 THEN 'nobody or Remy' ELSE 'other' END",
               {
                   {"Adam", "nobody or Remy"}, {"Cyrus", "nobody or Remy"},
                   {"Doruk", "nobody or Remy"}, {"Luc", "nobody or Remy"},
                   {"Martina", "nobody or Remy"}, {"Maxime", "nobody or Remy"},
                   {"Remy", "other"}, {"Suhas", "nobody or Remy"},
               });
}

// A node is equal to the node it is, which is the comparison the equality operator answers
TEST_F(CaseEntitySubjectTest, comparesTwoEntitiesForEquality) {
    expectRows("MATCH (a:Person)-[:KNOWS_WELL]->(b) "
               "RETURN a.name, CASE a WHEN b THEN 'itself' ELSE 'another' END",
               {{"Adam", "another"}, {"Remy", "another"}});
}

TEST_F(CaseEntitySubjectTest, comparesAnEntityAgainstItself) {
    expectRows("MATCH (a:Person)-[:KNOWS_WELL]->(b) "
               "RETURN a.name, CASE a WHEN a THEN 'itself' ELSE 'another' END",
               {{"Adam", "itself"}, {"Remy", "itself"}});
}

// Remy is node 0, so the row Adam's edge leads to him takes the branch naming that id
TEST_F(CaseEntitySubjectTest, comparesAnEntityAgainstAnID) {
    expectRows("MATCH (a:Person)-[:KNOWS_WELL]->(b) "
               "RETURN a.name, CASE b WHEN 0 THEN 'Remy' ELSE 'someone else' END",
               {{"Adam", "Remy"}, {"Remy", "someone else"}});
}

// Nothing orders a node or an edge, so a branch that compares one with an ordering
// operator is reported rather than answered
TEST_F(CaseEntitySubjectTest, rejectsAnOrderingBranchOverAnEntity) {
    runQueryExpectingError("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
                           "RETURN CASE r WHEN < 3 THEN 'low' ELSE 'high' END",
                           "cannot order one");
}

// A node is equal to a node or to an id, and to nothing else
TEST_F(CaseEntitySubjectTest, rejectsAValueNoEntityCompares) {
    runQueryExpectingError("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
                           "RETURN CASE r WHEN 'Remy' THEN 'low' ELSE 'high' END",
                           "not to 'String'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
