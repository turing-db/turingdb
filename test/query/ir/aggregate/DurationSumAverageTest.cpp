#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// sum() and avg() over durations, typed and held in type-erased cells. Every expected row
// is Neo4j 5.26's answer, except a sum of no duration: Neo4j answers the integer 0, and a
// typed duration sum answers the zero of its own type, PT0S.
class DurationSumAverageTest : public WriteQueryTest {
protected:
    void spendHoursOnInterests() {
        applyWrite("MATCH ()-[e:INTERESTED_IN]->() WHERE e.duration IS NOT NULL "
                   "SET e.spent = duration({hours: e.duration})");
    }

    void aggregateOps(std::string_view query, std::string& ops) {
        RowSink sink;
        const QueryStatus status = runQuery("EXPLAIN (nl) " + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string program;
        for (const Row& row : sink.rows()) {
            if (row.front() == "nl") {
                program = row.back();
            }
        }

        std::istringstream lines(program);
        std::string line;
        while (std::getline(lines, line)) {
            if (line.find("aggregate") != std::string::npos) {
                ops += line.substr(line.find_first_not_of(' '));
                ops += '\n';
            }
        }
    }

    void expectAggregateOps(std::string_view query, std::string_view expected) {
        std::string ops;
        aggregateOps(query, ops);

        EXPECT_EQ(ops, expected) << "query: " << query;
    }
};

TEST_F(DurationSumAverageTest, sumsAndAveragesDurations) {
    expectRows("UNWIND [duration({hours: 1}), duration({minutes: 30})] AS d RETURN sum(d), avg(d), min(d), max(d)",
               {{"PT1H30M", "PT45M", "PT30M", "PT1H"}});
    expectRows("UNWIND [duration({days: 1}), duration({hours: 1}), duration({hours: 2})] AS d RETURN avg(d)",
               {{"PT9H"}});
    expectRows("UNWIND [duration({seconds: 1}), duration({seconds: 0})] AS d RETURN avg(d)", {{"PT0.5S"}});
}

TEST_F(DurationSumAverageTest, skipsNullDurations) {
    expectRows("UNWIND [duration({hours: 1}), null] AS d RETURN sum(d), avg(d)", {{"PT1H", "PT1H"}});
}

TEST_F(DurationSumAverageTest, sumsAndAveragesDistinctDurations) {
    expectRows("UNWIND [duration({hours: 1}), duration({hours: 2})] AS d RETURN sum(DISTINCT d), count(DISTINCT d)",
               {{"PT3H", "2"}});
    expectRows("UNWIND [duration({hours: 1}), duration({hours: 1}), duration({hours: 2})] AS d "
               "RETURN sum(DISTINCT d), avg(DISTINCT d), sum(d), avg(d)",
               {{"PT3H", "PT1H30M", "PT4H", "PT1H20M"}});
}

// Neo4j keeps nanoseconds and truncates an average finer than one toward zero: the mean
// of 2, 0 and 0 nanoseconds is PT0S. Durations here keep microseconds, so the mean of 1
// and 0 microseconds, PT0.0000005S in Neo4j, truncates to PT0S the same way.
TEST_F(DurationSumAverageTest, truncatesAnAverageFinerThanAMicrosecondTowardZero) {
    expectRows("UNWIND [duration({microseconds: 1}), duration({microseconds: 0})] AS d RETURN avg(d)", {{"PT0S"}});
    expectRows("UNWIND [duration({microseconds: -1}), duration({microseconds: 0})] AS d RETURN avg(d)", {{"PT0S"}});
    expectRows("UNWIND [duration({microseconds: 2}), duration({microseconds: 0}), duration({microseconds: 0})] AS d "
               "RETURN avg(d)",
               {{"PT0S"}});
    expectRows("UNWIND [{d: duration({microseconds: 1})}, {d: duration({microseconds: 0})}] AS m RETURN avg(m.d)",
               {{"PT0S"}});
}

TEST_F(DurationSumAverageTest, sumsNoDurationToZeroAndAveragesItToNull) {
    expectRows("UNWIND [] AS x RETURN sum(duration({hours: x})), avg(duration({hours: x}))", {{"PT0S", "null"}});
    expectRows("UNWIND [1] AS x WITH x WHERE false RETURN sum(duration({hours: x})), avg(duration({hours: x}))",
               {{"PT0S", "null"}});
    expectRows("MATCH (n:Nothing) RETURN sum(n.d), avg(n.d)", {{"0", "null"}});
}

TEST_F(DurationSumAverageTest, keepsTheDeviationsAndThePercentilesFromDurations) {
    expectError("UNWIND [duration({hours: 1}), duration({hours: 2})] AS d RETURN stDev(d)",
                "Invalid arguments for function 'stDev'");
    expectError("UNWIND [duration({hours: 1}), duration({hours: 2})] AS d RETURN stDevP(d)",
                "Invalid arguments for function 'stDevP'");
    expectError("UNWIND [duration({hours: 1}), duration({hours: 2})] AS d RETURN percentileCont(d, 0.5)",
                "Invalid arguments for function 'percentileCont'");
    expectError("UNWIND [duration({hours: 1}), duration({hours: 2})] AS d RETURN percentileDisc(d, 0.5)",
                "Invalid arguments for function 'percentileDisc'");
    expectError("UNWIND [duration({hours: 1}), 2] AS d RETURN stDev(d)", "stDev() and stDevP() read numbers, not 'Duration'");
}

TEST_F(DurationSumAverageTest, groupsTheSumAndTheAverageOfStoredDurations) {
    spendHoursOnInterests();

    expectRows("MATCH (a:Person)-[e:INTERESTED_IN]->() RETURN a.name, sum(e.spent), avg(e.spent)",
               {{"Adam", "PT0S", "null"},
                {"Cyrus", "PT0S", "null"},
                {"Doruk", "PT0S", "null"},
                {"Luc", "PT35H", "PT17H30M"},
                {"Martina", "PT10H", "PT10H"},
                {"Maxime", "PT0S", "null"},
                {"Remy", "PT40H", "PT20H"},
                {"Suhas", "PT0S", "null"}});
    expectRows("MATCH ()-[e:INTERESTED_IN]->() RETURN sum(e.spent), avg(e.spent)", {{"PT85H", "PT17H"}});
}

TEST_F(DurationSumAverageTest, collectsAndOrdersDurations) {
    spendHoursOnInterests();

    expectRows("UNWIND [duration({hours: 1}), duration({minutes: 30})] AS d RETURN collect(d)", {{"[PT1H, PT30M]"}});
    expectRows("UNWIND [{d: duration({hours: 1})}, {d: duration({minutes: 30})}] AS m RETURN collect(m.d)",
               {{"[PT1H, PT30M]"}});
    expectRows("MATCH (a:Person)-[e:INTERESTED_IN]->() RETURN a.name, min(e.spent), max(e.spent)",
               {{"Adam", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"},
                {"Luc", "PT15H", "PT20H"},
                {"Martina", "PT10H", "PT10H"},
                {"Maxime", "null", "null"},
                {"Remy", "PT20H", "PT20H"},
                {"Suhas", "null", "null"}});
}

TEST_F(DurationSumAverageTest, groupsTheSumAndTheAverageOfADurationExpression) {
    expectRows("MATCH (a:Person)-[e:INTERESTED_IN]->() WHERE e.duration IS NOT NULL "
               "RETURN a.name, sum(duration({hours: e.duration})), avg(duration({hours: e.duration}))",
               {{"Luc", "PT35H", "PT17H30M"},
                {"Martina", "PT10H", "PT10H"},
                {"Remy", "PT40H", "PT20H"}});
}

TEST_F(DurationSumAverageTest, projectsTheSumAndTheAverageInAWith) {
    spendHoursOnInterests();

    expectRows("MATCH (a:Person)-[e:INTERESTED_IN]->() "
               "WITH a.name AS name, sum(e.spent) AS total, avg(e.spent) AS mean WHERE mean IS NOT NULL "
               "RETURN name, total + duration({hours: 1}), mean",
               {{"Luc", "PT36H", "PT17H30M"},
                {"Martina", "PT11H", "PT10H"},
                {"Remy", "PT41H", "PT20H"}});
    expectRows("MATCH ()-[e:INTERESTED_IN]->() WITH sum(e.spent) AS total, avg(e.spent) AS mean RETURN total, mean",
               {{"PT85H", "PT17H"}});
}

TEST_F(DurationSumAverageTest, skipsTheNullsAnOptionalMatchLeaves) {
    spendHoursOnInterests();

    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[e:INTERESTED_IN]->(:Interest {name: 'Computers'}) "
               "RETURN p.name, sum(e.spent), avg(e.spent)",
               {{"Adam", "PT0S", "null"},
                {"Cyrus", "PT0S", "null"},
                {"Doruk", "PT0S", "null"},
                {"Luc", "PT15H", "PT15H"},
                {"Martina", "PT0S", "null"},
                {"Maxime", "PT0S", "null"},
                {"Remy", "PT0S", "null"},
                {"Suhas", "PT0S", "null"}});
}

TEST_F(DurationSumAverageTest, groupsStoredDurationsOverChunksOfFewRows) {
    spendHoursOnInterests();

    for (const size_t chunkSize : {1, 2, 3}) {
        _interpreter->setChunkSize(chunkSize);

        expectRows("MATCH (a:Person)-[e:INTERESTED_IN]->() WHERE e.spent IS NOT NULL "
                   "RETURN a.name, sum(e.spent), avg(e.spent), sum(DISTINCT e.spent), avg(DISTINCT e.spent)",
                   {{"Luc", "PT35H", "PT17H30M", "PT35H", "PT17H30M"},
                    {"Martina", "PT10H", "PT10H", "PT10H", "PT10H"},
                    {"Remy", "PT40H", "PT20H", "PT20H", "PT20H"}});
        expectRows("MATCH ()-[e:INTERESTED_IN]->() RETURN sum(e.spent), avg(e.spent)", {{"PT85H", "PT17H"}});
    }
}

TEST_F(DurationSumAverageTest, sumsAndAveragesDurationsPastTheFirstChunk) {
    expectRows("UNWIND [x IN range(1, 70000) | duration({seconds: x})] + [null, null] AS d "
               "RETURN sum(d), avg(d), count(d)",
               {{"PT680565H16M40S", "PT9H43M20.5S", "70000"}});
    expectRows("UNWIND [x IN range(1, 70000) | duration({seconds: x % 7})] AS d RETURN sum(DISTINCT d), avg(DISTINCT d)",
               {{"PT21S", "PT3S"}});
    expectRows("UNWIND range(1, 70000) AS x RETURN x % 3, sum(duration({seconds: x})), avg(duration({seconds: x}))",
               {{"0", "PT226855H5M33S", "PT9H43M21S"},
                {"1", "PT226861H34M27S", "PT9H43M20.5S"},
                {"2", "PT226848H36M40S", "PT9H43M20S"}});
}

TEST_F(DurationSumAverageTest, sumsAndAveragesDurationsHeldInCells) {
    expectRows("UNWIND [{d: duration({hours: 1})}, {d: duration({minutes: 30})}] AS m "
               "RETURN sum(m.d), avg(m.d), min(m.d), max(m.d)",
               {{"PT1H30M", "PT45M", "PT30M", "PT1H"}});
    expectRows("UNWIND [{d: duration({days: 1})}, {d: duration({hours: 1})}, {d: duration({hours: 2})}] AS m "
               "RETURN avg(m.d)",
               {{"PT9H"}});
    expectRows("UNWIND [{d: duration({seconds: 1})}, {d: duration({seconds: 0})}] AS m RETURN avg(m.d)", {{"PT0.5S"}});
    expectRows("UNWIND [{d: duration({hours: 1})}, {d: null}] AS m RETURN sum(m.d), avg(m.d)", {{"PT1H", "PT1H"}});
    expectRows("UNWIND [{d: duration({hours: 1})}, {d: duration({hours: 2})}, {d: duration({hours: 1})}] AS m "
               "RETURN sum(DISTINCT m.d), avg(DISTINCT m.d), count(DISTINCT m.d)",
               {{"PT3H", "PT1H30M", "2"}});
    expectRows("UNWIND [{d: null}] AS m RETURN sum(m.d), avg(m.d)", {{"0", "null"}});
}

TEST_F(DurationSumAverageTest, groupsDurationsAndNumbersHeldInCells) {
    expectRows("UNWIND [{k: 1, d: duration({hours: 1})}, {k: 1, d: duration({hours: 2})}, "
               "{k: 2, d: 3}, {k: 2, d: 4.5}, {k: 3, d: null}] AS m "
               "RETURN m.k, sum(m.d), avg(m.d)",
               {{"1", "PT3H", "PT1H30M"},
                {"2", "7.500000", "3.750000"},
                {"3", "0", "null"}});
}

TEST_F(DurationSumAverageTest, sumsAndAveragesCellsPastTheFirstChunk) {
    expectRows("UNWIND [x IN range(1, 70000) | {d: duration({seconds: x})}] + [{d: null}] AS m RETURN sum(m.d), avg(m.d)",
               {{"PT680565H16M40S", "PT9H43M20.5S"}});
    expectRows("UNWIND [x IN range(1, 70000) | {k: x % 2, d: duration({seconds: x})}] + [{k: 0, d: null}] AS m "
               "RETURN m.k, sum(m.d), avg(m.d)",
               {{"0", "PT340287H30M", "PT9H43M21S"},
                {"1", "PT340277H46M40S", "PT9H43M20S"}});
}

TEST_F(DurationSumAverageTest, rejectsAGroupMixingNumbersAndDurations) {
    expectError("UNWIND [duration({hours: 1}), 2] AS d RETURN sum(d)", "sum() and avg() cannot add 'Integer' to a sum of 'Duration'");
    expectError("UNWIND [2, duration({hours: 1})] AS d RETURN sum(d)", "sum() and avg() cannot add 'Duration' to a sum of 'Integer'");
    expectError("UNWIND [0, duration({hours: 1})] AS d RETURN sum(d)", "sum() and avg() cannot add 'Duration' to a sum of 'Integer'");
    expectError("UNWIND [duration({hours: 1}), 2.5] AS d RETURN sum(d)", "sum() and avg() cannot add 'Double' to a sum of 'Duration'");
    expectError("UNWIND [duration({hours: 1}), 2] AS d RETURN avg(d)", "sum() and avg() cannot add 'Integer' to a sum of 'Duration'");
    expectError("UNWIND [2, duration({hours: 1})] AS d RETURN avg(d)", "sum() and avg() cannot add 'Duration' to a sum of 'Double'");
    expectError("UNWIND [{d: 1}, {d: duration({hours: 1})}] AS m RETURN sum(m.d)", "sum() and avg() cannot add 'Duration' to a sum of 'Integer'");
    expectError("UNWIND [{k: 1, d: 1}, {k: 1, d: duration({hours: 1})}] AS m RETURN m.k, avg(m.d)",
                "sum() and avg() cannot add 'Duration' to a sum of 'Double'");
}

TEST_F(DurationSumAverageTest, rejectsAMixPastTheFirstChunk) {
    expectError("UNWIND [x IN range(1, 70000) | duration({seconds: x})] + [1] AS d RETURN sum(d)",
                "sum() and avg() cannot add 'Integer' to a sum of 'Duration'");
    expectError("UNWIND [x IN range(1, 70000) | duration({seconds: x})] + [1] AS d RETURN avg(d)",
                "sum() and avg() cannot add 'Integer' to a sum of 'Duration'");
    expectError("UNWIND [x IN range(1, 70000) | {k: x % 2, d: duration({seconds: x})}] + [{k: 0, d: null}, {k: 1, d: 1}] AS m "
                "RETURN m.k, sum(m.d)",
                "sum() and avg() cannot add 'Integer' to a sum of 'Duration'");
}

TEST_F(DurationSumAverageTest, rejectsACellHoldingNeitherANumberNorADuration) {
    expectError("UNWIND [duration({hours: 1}), 'a'] AS d RETURN avg(d)", "sum() and avg() read numbers or durations, not 'String'");
    expectError("UNWIND [duration({hours: 1}), true] AS d RETURN sum(d)", "sum() and avg() read numbers or durations, not 'Bool'");
    expectError("UNWIND [1, 'a'] AS d RETURN sum(d)", "sum() and avg() read numbers or durations, not 'String'");
}

TEST_F(DurationSumAverageTest, keepsTheProgramOfASumOrAnAverageOfNumbers) {
    expectAggregateOps("UNWIND [1, 2] AS x RETURN sum(x), avg(x)",
                       "%0 = nl.aggregate avg : !nl.aggregate_state<f64>\n"
                       "%1 = nl.aggregate sum : !nl.aggregate_state<i64>\n"
                       "nl.aggregate_update sum %1, %arg0 : !nl.aggregate_state<i64>, !nl.chunk<!storage.nullable<i64>>\n"
                       "nl.aggregate_update avg %0, %arg0 : !nl.aggregate_state<f64>, !nl.chunk<!storage.nullable<i64>>\n"
                       "%3 = nl.aggregate_result avg(%0) : !nl.aggregate_state<f64> -> !nl.chunk<!storage.nullable<f64>>\n"
                       "%4 = nl.aggregate_result sum(%1) : !nl.aggregate_state<i64> -> !nl.chunk<!storage.nullable<i64>>\n");
    expectAggregateOps("UNWIND [1.5, 2.5] AS x RETURN sum(x), avg(x)",
                       "%0 = nl.aggregate avg : !nl.aggregate_state<f64>\n"
                       "%1 = nl.aggregate sum : !nl.aggregate_state<f64>\n"
                       "nl.aggregate_update sum %1, %arg0 : !nl.aggregate_state<f64>, !nl.chunk<!storage.nullable<f64>>\n"
                       "nl.aggregate_update avg %0, %arg0 : !nl.aggregate_state<f64>, !nl.chunk<!storage.nullable<f64>>\n"
                       "%3 = nl.aggregate_result avg(%0) : !nl.aggregate_state<f64> -> !nl.chunk<!storage.nullable<f64>>\n"
                       "%4 = nl.aggregate_result sum(%1) : !nl.aggregate_state<f64> -> !nl.chunk<!storage.nullable<f64>>\n");
    expectAggregateOps("MATCH (n:Person) RETURN n.isFrench, sum(n.age), avg(n.age)",
                       "%0 = nl.group_aggregate_buffer keys 1 aggregates [sum, avg]\n"
                       "nl.group_aggregate_update %0, (%5, %6, %6) : !nl.chunk<!storage.nullable<i1>>, "
                       "!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>\n"
                       "%4 = nl.group_aggregate(%0) : !nl.iter<!nl.chunk<!storage.nullable<i1>>, "
                       "!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<f64>>>\n");
    expectAggregateOps("UNWIND [1.5, 2.5] AS x RETURN x > 2.0, sum(x), avg(x)",
                       "%0 = nl.group_aggregate_buffer keys 1 aggregates [sum, avg]\n"
                       "nl.group_aggregate_update %0, (%4, %arg0, %arg0) : !nl.chunk<!storage.nullable<i1>>, "
                       "!nl.chunk<!storage.nullable<f64>>, !nl.chunk<!storage.nullable<f64>>\n"
                       "%3 = nl.group_aggregate(%0) : !nl.iter<!nl.chunk<!storage.nullable<i1>>, "
                       "!nl.chunk<!storage.nullable<f64>>, !nl.chunk<!storage.nullable<f64>>>\n");
}
