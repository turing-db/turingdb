#include <gtest/gtest.h>

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

// Two components of one instant are two different values, so a key naming one is not the
// column projecting the other. The instants below order differently by year than by day,
// which is what tells the two keys apart.
class DateTimeComponentOrderingTest : public TuringTest {
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
        write("CREATE (n:Event {name: 'c', at: datetime('2022-07-28T00:00:00Z')})");
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

    void expectOrderedRows(std::string_view query, const Rows& expected) {
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

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

// Days 15, 2, 28 order b, a, c; years 2026, 2019, 2022 order b, c, a. Ordering by the day
// and projecting the year must answer the day's order.
TEST_F(DateTimeComponentOrderingTest, ordersByTheComponentTheKeyNames) {
    expectOrderedRows("MATCH (n:Event) RETURN n.at.year ORDER BY n.at.day ASC",
                      {{"2019"}, {"2026"}, {"2022"}});
}

TEST_F(DateTimeComponentOrderingTest, ordersByTheComponentTheKeyNamesDescending) {
    expectOrderedRows("MATCH (n:Event) RETURN n.at.year ORDER BY n.at.day DESC",
                      {{"2022"}, {"2026"}, {"2019"}});
}

TEST_F(DateTimeComponentOrderingTest, ordersByTheProjectedComponentItself) {
    expectOrderedRows("MATCH (n:Event) RETURN n.at.year ORDER BY n.at.year ASC",
                      {{"2019"}, {"2022"}, {"2026"}});
}

// Months 1, 3, 7 order a, b, c; years 2026, 2019, 2022 order b, c, a
TEST_F(DateTimeComponentOrderingTest, ordersByAComponentOfABoundInstant) {
    expectOrderedRows("MATCH (n:Event) WITH n.at AS d RETURN d.year ORDER BY d.month ASC",
                      {{"2026"}, {"2019"}, {"2022"}});
}

TEST_F(DateTimeComponentOrderingTest, ordersByTheProjectedComponentOfABoundInstant) {
    expectOrderedRows("MATCH (n:Event) WITH n.at AS d RETURN d.year ORDER BY d.year ASC",
                      {{"2019"}, {"2022"}, {"2026"}});
}

// A component that is not the grouping key has no single value per group to order on, so
// it is rejected rather than read off the key's column
TEST_F(DateTimeComponentOrderingTest, rejectsGroupingOnOneComponentAndOrderingOnAnother) {
    expectError("MATCH (n:Event) RETURN n.at.year, count(*) ORDER BY n.at.day",
                "ORDER BY");
}

TEST_F(DateTimeComponentOrderingTest, ordersAGroupedQueryByItsOwnGroupingComponent) {
    expectOrderedRows("MATCH (n:Event) RETURN n.at.year, count(*) ORDER BY n.at.year DESC",
                      {{"2026", "1"}, {"2022", "1"}, {"2019", "1"}});
}
