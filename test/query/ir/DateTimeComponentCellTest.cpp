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

// An UNWIND hands its elements on as type-erased cells, which carry their type per row
// rather than per column. A calendar field read off one has to read the instant out of the
// cell, as size() and id() read a list and an entity out of one.
class DateTimeComponentCellTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            Graph* graph = system.createGraph(_graphName);
            SimpleGraph::createSimpleGraph(graph);
        }

        write("CREATE (n:Event {name: 'a', at: datetime('2026-01-15T00:00:00Z')})");
        write("CREATE (n:Event {name: 'b', at: datetime('2019-03-02T00:00:00Z')})");
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

TEST_F(DateTimeComponentCellTest, readsEveryFieldOfAnUnwoundInstant) {
    expectRows("UNWIND [datetime('2026-09-23T14:05:06.123456Z')] AS d "
               "RETURN d.year, d.month, d.day, d.hour, d.minute, d.second, "
               "d.millisecond, d.microsecond",
               {{"2026", "9", "23", "14", "5", "6", "123", "123456"}});
}

TEST_F(DateTimeComponentCellTest, readsAFieldOfEachUnwoundInstant) {
    expectRows("UNWIND [datetime('2026-01-15T00:00:00Z'), datetime('2019-03-02T00:00:00Z')] AS d "
               "RETURN d.year",
               {{"2026"}, {"2019"}});
}

// A cell holding no value answers null, as every other function over a cell does
TEST_F(DateTimeComponentCellTest, readsNullFromACellCarryingNoInstant) {
    expectRows("UNWIND [datetime('2026-01-15T00:00:00Z'), null] AS d RETURN d.year",
               {{"2026"}, {"null"}});
}

TEST_F(DateTimeComponentCellTest, readsAFieldOfTheCurrentInstantUnwound) {
    expectRows("UNWIND [datetime()] AS d RETURN d.year >= 2026", {{"true"}});
}

TEST_F(DateTimeComponentCellTest, readsAFieldOfAnInstantGatheredFromTheGraph) {
    expectRows("MATCH (n:Event) WITH collect(n.at) AS instants "
               "UNWIND instants AS d RETURN d.year",
               {{"2026"}, {"2019"}});
}

TEST_F(DateTimeComponentCellTest, filtersOnAFieldOfAnUnwoundInstant) {
    expectRows("UNWIND [datetime('2026-01-15T00:00:00Z'), datetime('2019-03-02T00:00:00Z')] AS d "
               "WITH d WHERE d.year > 2020 RETURN d.month",
               {{"1"}});
}
