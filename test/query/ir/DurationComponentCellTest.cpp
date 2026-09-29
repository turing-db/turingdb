#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A unit read off a duration an UNWIND handed on as a type-erased cell.
class DurationComponentCellTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            Graph* graph = system.createGraph(_graphName);
            SimpleGraph::createSimpleGraph(graph);
        }
    }

    void writeTasks() {
        write("CREATE (n:Task {name: 'a', took: duration(2000000)})");
        write("CREATE (n:Task {name: 'b', took: duration(90061000000)})");
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    void write(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(DurationComponentCellTest, readsEveryUnitOfAnUnwoundDuration) {
    expectRows("UNWIND [duration(90061123456)] AS d "
               "RETURN d.years, d.quarters, d.months, d.weeks, d.days, d.hours, d.minutes, "
               "d.seconds, d.milliseconds, d.microseconds",
               {{"0", "0", "0", "0", "1", "25", "1501", "90061", "90061123", "90061123456"}});
}

TEST_F(DurationComponentCellTest, readsAUnitOfEachUnwoundDuration) {
    expectRows("UNWIND [duration(90061000000), duration(-1500000)] AS d RETURN d.hours, d.seconds",
               {{"25", "90061"}, {"0", "-1"}});
}

TEST_F(DurationComponentCellTest, readsNullFromACellCarryingNoDuration) {
    expectRows("UNWIND [duration(2000000), null] AS d RETURN d.seconds",
               {{"2"}, {"null"}});
}

TEST_F(DurationComponentCellTest, readsAUnitOfADurationGatheredFromTheGraph) {
    writeTasks();

    expectRows("MATCH (n:Task) WITH collect(n.took) AS durations "
               "UNWIND durations AS d RETURN d.seconds",
               {{"2"}, {"90061"}});
}

TEST_F(DurationComponentCellTest, filtersOnAUnitOfAnUnwoundDuration) {
    expectRows("UNWIND [duration(90061000000), duration(2000000)] AS d "
               "WITH d WHERE d.hours > 0 RETURN d.minutes",
               {{"1501"}});
}

TEST_F(DurationComponentCellTest, readsARemainderOfAnUnwoundDuration) {
    expectRows("UNWIND [duration(90061123456), duration(-90000000), null] AS d "
               "RETURN d.minutesOfHour, d.secondsOfMinute",
               {{"1", "1"}, {"-1", "-30"}, {"null", "null"}});
}
