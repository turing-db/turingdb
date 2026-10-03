#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

}

// The end label of a variable-length pattern folded into the exploration by the
// fuse_explore_end_constraint pass: the rows are those of the unconstrained walk that end
// on a node carrying the label, read off the engine itself rather than hand-listed
class ExploreEndConstraintCypherTest : public CallV3Test {
protected:
    void namesOfLabel(std::string_view label, std::set<std::string>& names) {
        StringRowSink sink;
        runQuery(std::string("MATCH (m:") + std::string(label) + ") RETURN m.name", sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            names.insert(row.front());
        }
        ASSERT_FALSE(names.empty());
    }

    // The constrained query must emit exactly the rows of the unconstrained one whose end
    // column - the last - names a node of the label
    void expectEndsOnTheLabel(std::string_view constrained, std::string_view unconstrained, std::string_view label) {
        std::set<std::string> names;
        namesOfLabel(label, names);

        StringRowSink all;
        runQuery(unconstrained, all);

        Rows expected;
        for (const StringRowSink::Row& row : all.getRows()) {
            if (names.contains(row.back())) {
                expected.push_back(row);
            }
        }
        std::sort(expected.begin(), expected.end());

        StringRowSink sink;
        runQuery(constrained, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_FALSE(expected.empty()) << constrained;
        EXPECT_EQ(rows, expected) << constrained;
    }

    std::string_view dumpOf(const StringRowSink& sink, std::string_view stage) {
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                return row.back();
            }
        }

        return {};
    }
};

TEST_F(ExploreEndConstraintCypherTest, explainShowsTheFusedExploration) {
    StringRowSink sink;
    runQuery("EXPLAIN (around fuse_explore_end_constraint) MATCH (n:Person)-[e]->{2,4}(m:Interest) RETURN n.name, e, m.name", sink);

    const std::string_view before = dumpOf(sink, "before fuse_explore_end_constraint");
    EXPECT_TRUE(contains(before, "db.check_label_constraint")) << before;
    EXPECT_TRUE(contains(before, "db.filter")) << before;
    EXPECT_FALSE(contains(before, "end_labels")) << before;

    const std::string_view after = dumpOf(sink, "after fuse_explore_end_constraint");
    EXPECT_TRUE(contains(after, "end_labels [\"Interest\"]")) << after;
    EXPECT_FALSE(contains(after, "db.check_label_constraint")) << after;
    EXPECT_FALSE(contains(after, "db.filter")) << after;
}

TEST_F(ExploreEndConstraintCypherTest, keepsTheRowsOfTheWalkThatEndOnTheLabel) {
    expectEndsOnTheLabel("MATCH (n)-[e]->*(m:Person) RETURN n.name, e, m.name",
                         "MATCH (n)-[e]->*(m) RETURN n.name, e, m.name",
                         "Person");

    expectEndsOnTheLabel("MATCH (n:Person)-[e]-{0,2}(m:Interest) RETURN n.name, e, m.name",
                         "MATCH (n:Person)-[e]-{0,2}(m) RETURN n.name, e, m.name",
                         "Interest");

    expectEndsOnTheLabel("MATCH (n)<-[e]-{1,3}(m:Supernatural) RETURN n.name, e, m.name",
                         "MATCH (n)<-[e]-{1,3}(m) RETURN n.name, e, m.name",
                         "Supernatural");
}

TEST_F(ExploreEndConstraintCypherTest, keepsTheTypedAndPredicatedHops) {
    expectEndsOnTheLabel("MATCH (n:Person)-[e:KNOWS_WELL]->*(m:Person) RETURN n.name, e, m.name",
                         "MATCH (n:Person)-[e:KNOWS_WELL]->*(m) RETURN n.name, e, m.name",
                         "Person");

    expectEndsOnTheLabel("MATCH (n:Person)((a)-[e]->(b) WHERE b.age > 30){1,3}(m:Person) RETURN n.name, e, m.name",
                         "MATCH (n:Person)((a)-[e]->(b) WHERE b.age > 30){1,3}(m) RETURN n.name, e, m.name",
                         "Person");
}

TEST_F(ExploreEndConstraintCypherTest, countsTheConstrainedEndsAlone) {
    std::set<std::string> persons;
    namesOfLabel("Person", persons);

    StringRowSink all;
    runQuery("MATCH (n)-[e]->*(m) RETURN m.name", all);

    size_t expected = 0;
    for (const StringRowSink::Row& row : all.getRows()) {
        expected += persons.contains(row.front()) ? 1 : 0;
    }

    StringRowSink sink;
    runQuery("MATCH (n)-[e]->*(m:Person) RETURN count(*)", sink);

    ASSERT_EQ(sink.getRows().size(), 1u);
    EXPECT_EQ(sink.getRows().front().front(), std::to_string(expected));
}

TEST_F(ExploreEndConstraintCypherTest, continuesPastTheConstrainedEnd) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->{2,4}(m:Interest)-->(p:Person) RETURN n.name, e, m.name, p.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // variable-length-paths-6.json
    Rows expected {
        {"Adam", "4, 1", "Ghosts", "Remy"},
        {"Remy", "0, 4, 1", "Ghosts", "Remy"},
    };
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(rows, expected);
}
