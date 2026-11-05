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

class PointSpatialFunctionsTest : public TuringTest {
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

TEST_F(PointSpatialFunctionsTest, measuresACartesianDistance) {
    expectRows("RETURN point.distance(point({x: 1, y: 2}), point({x: 4, y: 6}))", {{"5.000000"}});
    expectRows("RETURN point.distance(point({x: 1, y: 2, z: 3}), point({x: 4, y: 6, z: 15}))", {{"13.000000"}});
}

TEST_F(PointSpatialFunctionsTest, measuresAGreatCircleDistanceInMetres) {
    expectRows("RETURN point.distance(point({longitude: 2.3522, latitude: 48.8566}), "
               "point({longitude: -0.1276, latitude: 51.5072}))",
               {{"343530.339999"}});
    expectRows("RETURN point.distance(point({longitude: 0, latitude: 0}), point({longitude: 1, latitude: 0}))",
               {{"111195.080234"}});
}

TEST_F(PointSpatialFunctionsTest, measuresAcrossTheAntimeridian) {
    expectRows("RETURN point.distance(point({longitude: 179.5, latitude: 0}), point({longitude: -179.5, latitude: 0}))",
               {{"111195.080234"}});
}

TEST_F(PointSpatialFunctionsTest, measuresBetweenTwoHeights) {
    expectRows("RETURN point.distance(point({longitude: 2.3522, latitude: 48.8566, height: 35}), "
               "point({longitude: -0.1276, latitude: 51.5072, height: 100}))",
               {{"343533.985807"}});
}

TEST_F(PointSpatialFunctionsTest, measuresNothingAcrossCRSs) {
    expectRows("RETURN point.distance(point({x: 1, y: 2}), point({x: 1, y: 2, crs: 'wgs-84'}))", {{"null"}});
    expectRows("RETURN point.distance(point({x: 1, y: 2}), point({x: 1, y: 2, z: 0}))", {{"null"}});
}

TEST_F(PointSpatialFunctionsTest, measuresNothingFromANull) {
    expectRows("RETURN point.distance(null, point({x: 1, y: 2}))", {{"null"}});
    expectRows("RETURN point.distance(point({x: 1, y: 2}), null)", {{"null"}});
}

TEST_F(PointSpatialFunctionsTest, filtersOnADistance) {
    write("CREATE (n:City {name: 'Paris', at: point({longitude: 2.3522, latitude: 48.8566})})");
    write("CREATE (n:City {name: 'London', at: point({longitude: -0.1276, latitude: 51.5072})})");
    write("CREATE (n:City {name: 'Versailles', at: point({longitude: 2.1301, latitude: 48.8049})})");

    expectRows("MATCH (a:City {name: 'Paris'}), (b:City) "
               "WHERE point.distance(a.at, b.at) < 50000 "
               "RETURN b.name",
               {{"Paris"}, {"Versailles"}});
}

TEST_F(PointSpatialFunctionsTest, measuresBetweenPointsAListHolds) {
    expectRows("UNWIND [[point({x: 0, y: 0}), point({x: 3, y: 4})], [point({x: 0, y: 0}), point({longitude: 0, latitude: 0})]] AS pair "
               "RETURN point.distance(pair[0], pair[1])",
               {{"5.000000"}, {"null"}});
}

TEST_F(PointSpatialFunctionsTest, rejectsADistanceToAValueThatIsNoPoint) {
    expectError("RETURN point.distance(point({x: 1, y: 2}), 1)", "point.distance");
    expectError("UNWIND [1] AS v RETURN point.distance(point({x: 1, y: 2}), v)", "point.distance");
}

TEST_F(PointSpatialFunctionsTest, findsACartesianPointInABox) {
    expectRows("RETURN point.withinBBox(point({x: 1, y: 2}), point({x: 0, y: 0}), point({x: 2, y: 2}))", {{"true"}});
    expectRows("RETURN point.withinBBox(point({x: 3, y: 2}), point({x: 0, y: 0}), point({x: 2, y: 2}))", {{"false"}});
    expectRows("RETURN point.withinBBox(point({x: 2, y: 0}), point({x: 0, y: 0}), point({x: 2, y: 2}))", {{"true"}});
    expectRows("RETURN point.withinBBox(point({x: 1, y: 1, z: 5}), point({x: 0, y: 0, z: 0}), point({x: 2, y: 2, z: 2}))",
               {{"false"}});
}

TEST_F(PointSpatialFunctionsTest, findsNoPointInABoxWhoseCornersAreSwapped) {
    expectRows("RETURN point.withinBBox(point({x: 1, y: 1}), point({x: 2, y: 2}), point({x: 0, y: 0}))", {{"false"}});
    expectRows("RETURN point.withinBBox(point({longitude: 0, latitude: 1}), "
               "point({longitude: -1, latitude: 2}), point({longitude: 1, latitude: 0}))",
               {{"false"}});
}

TEST_F(PointSpatialFunctionsTest, findsAGeographicPointInABoxAcrossTheAntimeridian) {
    expectRows("RETURN point.withinBBox(point({longitude: 179.5, latitude: 0}), "
               "point({longitude: 179, latitude: -1}), point({longitude: -179, latitude: 1}))",
               {{"true"}});
    expectRows("RETURN point.withinBBox(point({longitude: -179.5, latitude: 0}), "
               "point({longitude: 179, latitude: -1}), point({longitude: -179, latitude: 1}))",
               {{"true"}});
    expectRows("RETURN point.withinBBox(point({longitude: 0, latitude: 0}), "
               "point({longitude: 179, latitude: -1}), point({longitude: -179, latitude: 1}))",
               {{"false"}});
}

TEST_F(PointSpatialFunctionsTest, findsNothingAcrossCRSsOrFromANull) {
    expectRows("RETURN point.withinBBox(point({x: 1, y: 1}), point({x: 0, y: 0, crs: 'wgs-84'}), point({x: 2, y: 2}))",
               {{"null"}});
    expectRows("RETURN point.withinBBox(null, point({x: 0, y: 0}), point({x: 2, y: 2}))", {{"null"}});
    expectRows("RETURN point.withinBBox(point({x: 1, y: 1}), null, point({x: 2, y: 2}))", {{"null"}});
}

TEST_F(PointSpatialFunctionsTest, filtersOnABox) {
    write("CREATE (n:City {name: 'Paris', at: point({longitude: 2.3522, latitude: 48.8566})})");
    write("CREATE (n:City {name: 'London', at: point({longitude: -0.1276, latitude: 51.5072})})");

    expectRows("MATCH (n:City) "
               "WHERE point.withinBBox(n.at, point({longitude: 0, latitude: 45}), point({longitude: 5, latitude: 50})) "
               "RETURN n.name",
               {{"Paris"}});
}

TEST_F(PointSpatialFunctionsTest, ordersByDistance) {
    write("CREATE (n:City {name: 'Paris', at: point({longitude: 2.3522, latitude: 48.8566})})");
    write("CREATE (n:City {name: 'London', at: point({longitude: -0.1276, latitude: 51.5072})})");
    write("CREATE (n:City {name: 'Versailles', at: point({longitude: 2.1301, latitude: 48.8049})})");

    expectOrderedRows("MATCH (n:City) RETURN n.name "
                      "ORDER BY point.distance(n.at, point({longitude: 2.3522, latitude: 48.8566}))",
                      {{"Paris"}, {"Versailles"}, {"London"}});
}

TEST_F(PointSpatialFunctionsTest, measuresNothingFromAMissingProperty) {
    write("CREATE (n:City {name: 'Paris', at: point({longitude: 2.3522, latitude: 48.8566})})");
    write("CREATE (n:City {name: 'Nowhere'})");

    expectRows("MATCH (n:City) RETURN n.name, point.distance(n.at, point({longitude: 2.3522, latitude: 48.8566}))",
               {{"Paris", "0.000000"}, {"Nowhere", "null"}});
}

TEST_F(PointSpatialFunctionsTest, answersTheTicketQuery) {
    write("CREATE (:City {name: 'London', location: point({latitude: 51.5072, longitude: -0.1276})})");
    write("CREATE (:City {name: 'Paris', location: point({latitude: 48.8566, longitude: 2.3522})})");
    write("CREATE (:City {name: 'Croydon', location: point({latitude: 51.3762, longitude: -0.0982})})");

    expectRows("MATCH (a:City {name: 'London'}), (b:City) "
               "WHERE point.distance(a.location, b.location) < 50000 "
               "RETURN b.name, point.distance(a.location, b.location) AS metres",
               {{"London", "0.000000"}, {"Croydon", "14708.388652"}});
}
