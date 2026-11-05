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

class PointOrderAndAggregateTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
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
                                     &_env->getCompilerContext(),
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
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void run(std::string_view query, RowSink& sink, QueryStatus& status) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void createFourCities() {
        write("CREATE (n:City {name: 'Paris', at: point({longitude: 2.3522, latitude: 48.8566})})");
        write("CREATE (n:City {name: 'London', at: point({longitude: -0.1276, latitude: 51.5072})})");
        write("CREATE (n:City {name: 'Grid', at: point({x: 3, y: 4})})");
        write("CREATE (n:City {name: 'Tower', at: point({x: 3, y: 4, z: 5})})");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(PointOrderAndAggregateTest, comparesPointsOfOneCRSByCoordinates) {
    expectRows("RETURN point({x: 1, y: 9}) < point({x: 2, y: 0}), point({x: 1, y: 1}) < point({x: 1, y: 2})",
               {{"true", "true"}});
    expectRows("RETURN point({x: 1, y: 2, z: 3}) > point({x: 1, y: 2, z: 2})", {{"true"}});
    expectRows("RETURN point({x: 1, y: 2}) <= point({x: 1, y: 2}), point({x: 1, y: 2}) >= point({x: 1, y: 2})",
               {{"true", "true"}});
    expectRows("RETURN point({x: 1, y: 2}) < point({x: 1, y: 2}), point({x: 1, y: 2}) > point({x: 1, y: 2})",
               {{"false", "false"}});
}

TEST_F(PointOrderAndAggregateTest, comparesPointsAcrossCRSsBySRID) {
    expectRows("RETURN point({x: 9, y: 9, crs: 'wgs-84'}) < point({x: 0, y: 0})", {{"true"}});
    expectRows("RETURN point({x: 9, y: 9}) < point({x: 0, y: 0, z: 0})", {{"true"}});
    expectRows("RETURN point({x: 0, y: 0, z: 0, crs: 'wgs-84-3d'}) > point({x: 9, y: 9, crs: 'wgs-84'})", {{"true"}});
}

TEST_F(PointOrderAndAggregateTest, comparesNothingButAPointWithAPoint) {
    expectRows("RETURN point({x: 1, y: 2}) < 3, point({x: 1, y: 2}) < 'a'", {{"null", "null"}});
    expectRows("RETURN point({x: 1, y: 2}) < null", {{"null"}});
}

TEST_F(PointOrderAndAggregateTest, comparesPointsAListHolds) {
    expectRows("UNWIND [point({x: 1, y: 2}), point({x: 3, y: 4}), 5] AS p RETURN p, p < point({x: 2, y: 0})",
               {{"5", "null"},
                {"point({x: 1.0, y: 2.0, crs: 'cartesian'})", "true"},
                {"point({x: 3.0, y: 4.0, crs: 'cartesian'})", "false"}});
}

TEST_F(PointOrderAndAggregateTest, filtersOnAPointOrder) {
    createFourCities();
    expectRows("MATCH (n:City) WHERE n.at < point({x: 0, y: 0}) RETURN n.name", {{"Paris"}, {"London"}});
    expectRows("MATCH (n:City) WHERE n.at >= point({x: 3, y: 4}) RETURN n.name", {{"Grid"}, {"Tower"}});
}

TEST_F(PointOrderAndAggregateTest, takesTheLeastAndGreatestPoint) {
    createFourCities();
    expectRows("MATCH (n:City) RETURN min(n.at), max(n.at)",
               {{"point({x: -0.1276, y: 51.5072, crs: 'wgs-84'})", "point({x: 3.0, y: 4.0, z: 5.0, crs: 'cartesian-3d'})"}});
}

TEST_F(PointOrderAndAggregateTest, takesTheExtremesOfEachGroup) {
    write("CREATE (n:City {name: 'a', region: 'north', at: point({x: 1, y: 5})})");
    write("CREATE (n:City {name: 'b', region: 'north', at: point({x: 2, y: 0})})");
    write("CREATE (n:City {name: 'c', region: 'south', at: point({x: 7, y: 7})})");
    write("CREATE (n:City {name: 'd', region: 'south'})");

    expectRows("MATCH (n:City) RETURN n.region, min(n.at), max(n.at)",
               {{"north", "point({x: 1.0, y: 5.0, crs: 'cartesian'})", "point({x: 2.0, y: 0.0, crs: 'cartesian'})"},
                {"south", "point({x: 7.0, y: 7.0, crs: 'cartesian'})", "point({x: 7.0, y: 7.0, crs: 'cartesian'})"}});
    expectRows("MATCH (n:City) RETURN n.region, min(DISTINCT n.at)",
               {{"north", "point({x: 1.0, y: 5.0, crs: 'cartesian'})"}, {"south", "point({x: 7.0, y: 7.0, crs: 'cartesian'})"}});
}

TEST_F(PointOrderAndAggregateTest, takesTheExtremesOfPointsAListHolds) {
    expectRows("UNWIND [point({x: 3, y: 0}), point({x: 1, y: 9}), null] AS p RETURN min(p), max(p)",
               {{"point({x: 1.0, y: 9.0, crs: 'cartesian'})", "point({x: 3.0, y: 0.0, crs: 'cartesian'})"}});
}

TEST_F(PointOrderAndAggregateTest, takesNoExtremeOfNoPoint) {
    write("CREATE (n:City {name: 'a', at: point({x: 1, y: 2})})");
    expectRows("MATCH (n:City) WHERE n.name = 'b' RETURN min(n.at), max(n.at)", {{"null", "null"}});
}

TEST_F(PointOrderAndAggregateTest, averagesPointsIntoTheirCentroid) {
    write("CREATE (n:City {name: 'a', at: point({x: 0, y: 0})})");
    write("CREATE (n:City {name: 'b', at: point({x: 4, y: 0})})");
    write("CREATE (n:City {name: 'c', at: point({x: 2, y: 6})})");
    write("CREATE (n:City {name: 'd'})");

    expectRows("MATCH (n:City) RETURN avg(n.at)", {{"point({x: 2.0, y: 2.0, crs: 'cartesian'})"}});
}

TEST_F(PointOrderAndAggregateTest, averagesPointsOfEveryCRS) {
    expectRows("UNWIND [point({x: 0, y: 0, z: 0}), point({x: 2, y: 4, z: 6})] AS p RETURN avg(p)",
               {{"point({x: 1.0, y: 2.0, z: 3.0, crs: 'cartesian-3d'})"}});
    expectRows("UNWIND [point({longitude: 2, latitude: 48}), point({longitude: 4, latitude: 50})] AS p RETURN avg(p)",
               {{"point({x: 3.0, y: 49.0, crs: 'wgs-84'})"}});
}

TEST_F(PointOrderAndAggregateTest, averagesThePointsOfEachGroup) {
    write("CREATE (n:City {name: 'a', region: 'north', at: point({x: 0, y: 0})})");
    write("CREATE (n:City {name: 'b', region: 'north', at: point({x: 2, y: 2})})");
    write("CREATE (n:City {name: 'c', region: 'north', at: point({x: 2, y: 2})})");
    write("CREATE (n:City {name: 'd', region: 'south', at: point({x: 5, y: 5})})");

    expectRows("MATCH (n:City) RETURN n.region, avg(n.at)",
               {{"north", "point({x: 1.3333333333333333, y: 1.3333333333333333, crs: 'cartesian'})"},
                {"south", "point({x: 5.0, y: 5.0, crs: 'cartesian'})"}});
    expectRows("MATCH (n:City) RETURN n.region, avg(DISTINCT n.at)",
               {{"north", "point({x: 1.0, y: 1.0, crs: 'cartesian'})"}, {"south", "point({x: 5.0, y: 5.0, crs: 'cartesian'})"}});
}

TEST_F(PointOrderAndAggregateTest, averagesThePointsOfEachGroupAListHolds) {
    expectRows("UNWIND [['a', point({x: 0, y: 0})], ['a', point({x: 4, y: 2})], ['b', point({x: 1, y: 1})]] AS row "
               "RETURN row[0], avg(row[1])",
               {{"a", "point({x: 2.0, y: 1.0, crs: 'cartesian'})"}, {"b", "point({x: 1.0, y: 1.0, crs: 'cartesian'})"}});
}

TEST_F(PointOrderAndAggregateTest, averagesNoPointToNull) {
    write("CREATE (n:City {name: 'a', at: point({x: 1, y: 2})})");
    expectRows("MATCH (n:City) WHERE n.name = 'b' RETURN avg(n.at)", {{"null"}});
}

TEST_F(PointOrderAndAggregateTest, rejectsAnAverageAcrossCRSs) {
    createFourCities();
    expectError("MATCH (n:City) RETURN avg(n.at)", "avg() reads points of one CRS, not wgs-84 and cartesian");
    expectError("UNWIND [point({x: 1, y: 2}), point({x: 1, y: 2, z: 3})] AS p RETURN avg(p)",
                "avg() reads points of one CRS, not cartesian and cartesian-3d");
}

TEST_F(PointOrderAndAggregateTest, rejectsAnAverageOfPointsAndNumbers) {
    expectError("UNWIND [point({x: 1, y: 2}), 3] AS p RETURN avg(p)", "sum() and avg() cannot add 'Integer' to a sum of 'Point'");
    expectError("UNWIND [3, point({x: 1, y: 2})] AS p RETURN avg(p)", "sum() and avg() cannot add 'Point' to a sum of 'Double'");
}

TEST_F(PointOrderAndAggregateTest, rejectsASumOfPoints) {
    expectError("RETURN sum(point({x: 1, y: 2}))", "Invalid arguments for function 'sum'");
    expectError("UNWIND [point({x: 1, y: 2})] AS p RETURN sum(p)", "Invalid arguments for function 'sum'");
    expectError("UNWIND [point({x: 1, y: 2}), 1] AS p RETURN sum(p)", "sum() and avg() read numbers or durations, not 'Point'");
}
