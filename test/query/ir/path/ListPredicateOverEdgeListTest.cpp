#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// The variable of a variable-length relationship binds the list of its relationships, the
// list relationships(p) returns for the path, so a list comprehension or predicate reads it
class ListPredicateOverEdgeListTest : public CallV3Test {
protected:
    void sortedRows(const std::string& query, std::vector<StringRowSink::Row>& rows) {
        StringRowSink sink;
        runQuery(query, sink);

        rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
    }

    void expectSameRowsAsOverThePath(const std::string& overTheList, const std::string& returned) {
        const std::string match = "MATCH p = (a)-[r*1..2]->(b) ";

        std::vector<StringRowSink::Row> listRows;
        sortedRows(match + overTheList + " RETURN a.name, b.name, " + returned, listRows);

        std::string overThePath = overTheList;
        std::string returnedOverThePath = returned;
        for (std::string* text : {&overThePath, &returnedOverThePath}) {
            for (size_t position = text->find("IN r "); position != std::string::npos; position = text->find("IN r ", position)) {
                text->replace(position, 5, "IN relationships(p) ");
            }
        }

        std::vector<StringRowSink::Row> pathRows;
        sortedRows(match + overThePath + " RETURN a.name, b.name, " + returnedOverThePath, pathRows);

        EXPECT_FALSE(pathRows.empty()) << overThePath;
        EXPECT_EQ(listRows, pathRows) << overTheList;
    }
};

TEST_F(ListPredicateOverEdgeListTest, countsThePathsWhoseEveryRelationshipHolds) {
    StringRowSink sink;
    runQuery("MATCH (a)-[r*1..2]->(b) WHERE all(x IN r WHERE x.duration >= 20) RETURN count(b)", sink);

    const std::vector<StringRowSink::Row> expected {{"14"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ListPredicateOverEdgeListTest, keepsThePathsWhoseEveryRelationshipHolds) {
    expectSameRowsAsOverThePath("WHERE all(x IN r WHERE x.duration >= 20)", "size(r)");
}

TEST_F(ListPredicateOverEdgeListTest, keepsThePathsSomeRelationshipOfWhichHolds) {
    expectSameRowsAsOverThePath("WHERE any(x IN r WHERE x.duration > 100)", "size(r)");
}

TEST_F(ListPredicateOverEdgeListTest, keepsThePathsNoRelationshipOfWhichHolds) {
    expectSameRowsAsOverThePath("WHERE none(x IN r WHERE x.duration > 100)", "size(r)");
}

TEST_F(ListPredicateOverEdgeListTest, keepsThePathsOneRelationshipOfWhichHolds) {
    expectSameRowsAsOverThePath("WHERE single(x IN r WHERE x.duration = 200)", "size(r)");
}

TEST_F(ListPredicateOverEdgeListTest, projectsAPropertyOfEachRelationship) {
    expectSameRowsAsOverThePath("", "[x IN r | x.duration]");
}

TEST_F(ListPredicateOverEdgeListTest, keepsTheRelationshipsAWhereHolds) {
    expectSameRowsAsOverThePath("", "[x IN r WHERE x.duration >= 20 | x.duration]");
}

TEST_F(ListPredicateOverEdgeListTest, readsTheListCarriedThroughAWith) {
    StringRowSink sink;
    runQuery("MATCH (a)-[r*1..2]->(b) WITH a, b, r WHERE all(x IN r WHERE x.duration >= 20) RETURN count(b)", sink);

    const std::vector<StringRowSink::Row> expected {{"14"}};
    EXPECT_EQ(sink.getRows(), expected);
}
