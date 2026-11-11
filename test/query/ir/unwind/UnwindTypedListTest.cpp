#include <gtest/gtest.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

class UnwindTypedListTest : public CallV3Test {
protected:
    void expectSortedRows(const std::string& query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << query;
    }

    void expectRows(const std::string& query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectUnwindsInto(const std::string& query, std::string_view elementType) {
        StringRowSink sink;
        runQuery("EXPLAIN (nl) " + query, sink);

        const Rows& rows = sink.getRows();
        ASSERT_EQ(rows.size(), 1u);

        const std::string& program = rows.front().back();
        const std::string typedIterator = "!nl.iter<!nl.chunk<" + std::string(elementType) + ">";

        // The nl.for over an unwind's result spells the result type, whichever form the
        // unwind takes; nl.unwind_computed spells it after its region.
        std::string unwindTypes;
        std::string loopOverUnwind;
        std::istringstream lines(program);
        for (std::string line; std::getline(lines, line);) {
            const size_t assignment = line.find(" = nl.unwind");
            if (assignment != std::string::npos) {
                const size_t nameStart = line.find('%');
                loopOverUnwind = " in " + line.substr(nameStart, assignment - nameStart) + " :";
            } else if (!loopOverUnwind.empty() && line.contains("nl.for") && line.contains(loopOverUnwind)) {
                unwindTypes += line + "\n";
                loopOverUnwind.clear();
            }
        }

        EXPECT_TRUE(std::string_view(unwindTypes).contains(typedIterator)) << query << "\n" << program;
        EXPECT_FALSE(std::string_view(unwindTypes).contains("!nl.iter<!nl.chunk<!storage.any>")) << query << "\n" << program;
    }

    void writeTasks() {
        runWrite("CREATE (n:Task {name: 'a', took: duration(2000000), at: datetime('2024-03-01T10:00:00Z'), "
                 "where: point({x: 1.5, y: 2.5}), meta: {rank: 1}})");
        runWrite("CREATE (n:Task {name: 'b', took: duration(90061000000), at: datetime('2025-07-04T08:30:00Z'), "
                 "where: point({x: 3.5, y: 4.5}), meta: {rank: 2}})");
    }
};

TEST_F(UnwindTypedListTest, unwindsDateTimesIntoADateTimeColumn) {
    const std::string query = "UNWIND [datetime('2024-03-01T10:00:00Z'), datetime('2025-07-04T08:30:00Z')] AS d "
                              "RETURN d.year, d.month";

    expectSortedRows(query, {{"2024", "3"}, {"2025", "7"}});
    expectUnwindsInto(query, "!storage.nullable<!storage.datetime>");
}

TEST_F(UnwindTypedListTest, unwindsDurationsIntoADurationColumn) {
    const std::string query = "UNWIND [duration(1000000), duration(90061000000)] AS d RETURN d.hours, d.seconds";

    expectSortedRows(query, {{"0", "1"}, {"25", "90061"}});
    expectUnwindsInto(query, "!storage.nullable<!storage.duration>");
}

TEST_F(UnwindTypedListTest, unwindsADurationAndANullIntoADurationColumn) {
    const std::string query = "UNWIND [duration(2000000), null] AS d RETURN d.seconds";

    expectSortedRows(query, {{"2"}, {"null"}});
    expectUnwindsInto(query, "!storage.nullable<!storage.duration>");
}

TEST_F(UnwindTypedListTest, unwindsMapsIntoAMapColumn) {
    const std::string query = "UNWIND [{a: 1}, {a: 2}] AS m RETURN m.a";

    expectSortedRows(query, {{"1"}, {"2"}});
    expectUnwindsInto(query, "!storage.nullable<!storage.map>");
}

TEST_F(UnwindTypedListTest, unwindsPointsIntoAPointColumn) {
    const std::string query = "UNWIND [point({x: 1.5, y: 2.5}), point({x: 3.5, y: 4.5})] AS p RETURN p.x, p.y";

    expectSortedRows(query, {{"1.5", "2.5"}, {"3.5", "4.5"}});
    expectUnwindsInto(query, "!storage.nullable<!storage.point>");
}

TEST_F(UnwindTypedListTest, unwindsCollectedPropertiesIntoTypedColumns) {
    writeTasks();

    const std::string durations = "MATCH (n:Task) WITH collect(n.took) AS ds UNWIND ds AS d RETURN d.seconds";
    expectSortedRows(durations, {{"2"}, {"90061"}});
    expectUnwindsInto(durations, "!storage.nullable<!storage.duration>");

    const std::string dateTimes = "MATCH (n:Task) WITH collect(n.at) AS ds UNWIND ds AS d RETURN d.year";
    expectSortedRows(dateTimes, {{"2024"}, {"2025"}});
    expectUnwindsInto(dateTimes, "!storage.nullable<!storage.datetime>");

    const std::string points = "MATCH (n:Task) WITH collect(n.where) AS ps UNWIND ps AS p RETURN p.y";
    expectSortedRows(points, {{"2.5"}, {"4.5"}});
    expectUnwindsInto(points, "!storage.nullable<!storage.point>");

    const std::string maps = "MATCH (n:Task) WITH collect(n.meta) AS ms UNWIND ms AS m RETURN m.rank";
    expectSortedRows(maps, {{"1"}, {"2"}});
    expectUnwindsInto(maps, "!storage.nullable<!storage.map>");
}

TEST_F(UnwindTypedListTest, ordersAndDeduplicatesUnwoundTemporalValues) {
    expectRows("UNWIND [duration(3000000), duration(1000000), duration(3000000)] AS d "
               "RETURN DISTINCT d.seconds AS s ORDER BY s",
               {{"1"}, {"3"}});
    expectRows("UNWIND [datetime('2025-01-01T00:00:00Z'), datetime('2024-01-01T00:00:00Z')] AS d "
               "WITH d ORDER BY d RETURN d.year",
               {{"2024"}, {"2025"}});
    expectRows("UNWIND [duration(3000000), duration(1000000), duration(3000000)] AS d "
               "WITH d, count(*) AS c ORDER BY d RETURN d.seconds, c",
               {{"1", "1"}, {"3", "2"}});
}

TEST_F(UnwindTypedListTest, comparesUnwoundDateTimes) {
    expectSortedRows("UNWIND [datetime('2024-03-01T10:00:00Z'), datetime('2025-07-04T08:30:00Z')] AS d "
                     "WITH d WHERE d > datetime('2025-01-01T00:00:00Z') RETURN d.year",
                     {{"2025"}});
}

TEST_F(UnwindTypedListTest, addsADurationToUnwoundDateTimes) {
    expectSortedRows("UNWIND [datetime('2024-03-01T10:00:00Z'), datetime('2025-07-04T08:30:00Z')] AS d "
                     "RETURN (d + duration(86400000000)).day",
                     {{"2"}, {"5"}});
}

TEST_F(UnwindTypedListTest, unwindsEmbeddingsIntoAnEmbeddingColumn) {
    const std::string query = "UNWIND [(1, 5), (2, 3)] AS e RETURN e";

    expectSortedRows(query, {{"1, 5"}, {"2, 3"}});
    expectUnwindsInto(query, "!storage.nullable<!storage.embedding>");
}

TEST_F(UnwindTypedListTest, unwindsCollectedPathsIntoAPathColumn) {
    const std::string query = "MATCH p = (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) WITH collect(p) AS ps "
                              "UNWIND ps AS q RETURN length(q), nodes(q)[1].name";

    expectSortedRows(query, {{"1", "Adam"}});
    expectUnwindsInto(query, "!storage.path");
}

TEST_F(UnwindTypedListTest, unwindsAPathAndANullIntoAPathColumn) {
    const std::string query = "MATCH p = (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) "
                              "UNWIND [p, null] AS q RETURN q IS NULL, length(q)";

    expectSortedRows(query, {{"false", "1"}, {"true", "null"}});
    expectUnwindsInto(query, "!storage.path");
}

TEST_F(UnwindTypedListTest, ordersUnwoundPaths) {
    expectRows("MATCH p = (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) "
               "UNWIND [p, null] AS q WITH q ORDER BY q RETURN length(q)",
               {{"1"}, {"null"}});
}
