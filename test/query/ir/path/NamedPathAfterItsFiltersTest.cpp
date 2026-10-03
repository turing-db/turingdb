#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

}

// A named path is built over the rows the WHERE of its MATCH keeps, unless the WHERE reads
// the path: a filter that does not read it cuts the rows first, and the traversal ahead of
// it is optimised as it would be with no path named
class NamedPathAfterItsFiltersTest : public CallV3Test {
protected:
    void optimisedDump(std::string_view query, std::string& dump) {
        StringRowSink sink;
        runQuery(query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "db") {
                dump = row.back();
            }
        }
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sorted(expected)) << query;
    }
};

TEST_F(NamedPathAfterItsFiltersTest, buildsThePathAfterAFilterOnItsEnd) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->(m:Person) WHERE m.name = 'Adam' RETURN p", dump);

    const size_t lastFilter = dump.rfind("db.filter");
    const size_t pathBuild = dump.find("db.make_path");

    ASSERT_NE(pathBuild, std::string::npos) << dump;
    EXPECT_TRUE(lastFilter == std::string::npos || lastFilter < pathBuild) << dump;
}

TEST_F(NamedPathAfterItsFiltersTest, fusesTheEndOfAWalkAsWithNoPathNamed) {
    std::string withoutPath;
    optimisedDump("EXPLAIN (db) MATCH (n:Person {name: 'Adam'})-[x]->(k)-[e]->+(m) WHERE m.name = 'Adam' RETURN m", withoutPath);
    ASSERT_TRUE(contains(withoutPath, "end_nodes")) << withoutPath;

    std::string withPath;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person {name: 'Adam'})-[x]->(k)-[e]->+(m) WHERE m.name = 'Adam' RETURN p", withPath);
    EXPECT_TRUE(contains(withPath, "end_nodes")) << withPath;
}

TEST_F(NamedPathAfterItsFiltersTest, readsAFixedHopFilteredOnItsEnd) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WHERE m.name = 'Adam' RETURN p", {{"(0), [0], (1)"}});
}

TEST_F(NamedPathAfterItsFiltersTest, readsAPathFilteredOnBothEnds) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WHERE n.name = 'Remy' AND m.name = 'Adam' RETURN nodes(p)", {{"0, 1"}});
}

TEST_F(NamedPathAfterItsFiltersTest, readsAWalkFilteredOnItsEnd) {
    expectRows("MATCH p = (n:Person {name: 'Adam'})-[x]->(k)-[e]->+(m) WHERE m.name = 'Adam' RETURN p", {
        {"(1), [4], (0), [0], (1)"},
        {"(1), [4], (0), [1], (6), [7], (0), [0], (1)"},
    });
}

TEST_F(NamedPathAfterItsFiltersTest, keepsThePathAheadOfAFilterReadingIt) {
    expectRows("MATCH p = (n:Person)-[x]->(k:Person)-[e]->+(m:Person) WHERE n.name = 'Remy' AND length(p) = 2 RETURN nodes(p)",
               {{"0, 1, 0"}});
}

TEST_F(NamedPathAfterItsFiltersTest, readsAnOptionalPathFilteredOnItsEnd) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->(m:Person) WHERE m.name = 'Adam' RETURN n.name, nodes(p)", {
        {"Remy", "0, 1"},
        {"Adam", "null"},
        {"Maxime", "null"},
        {"Luc", "null"},
        {"Martina", "null"},
        {"Suhas", "null"},
        {"Cyrus", "null"},
        {"Doruk", "null"},
    });
}
