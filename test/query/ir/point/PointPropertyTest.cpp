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

class PointPropertyTest : public TuringTest {
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

TEST_F(PointPropertyTest, returnsACartesianPoint) {
    expectRows("RETURN point({x: 1, y: 2})", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
    expectRows("RETURN point({x: 1.5, y: -2, z: 3})", {{"point({x: 1.5, y: -2.0, z: 3.0, crs: 'cartesian-3d'})"}});
}

TEST_F(PointPropertyTest, returnsAGeographicPoint) {
    expectRows("RETURN point({longitude: 2.3522, latitude: 48.8566})",
               {{"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
    expectRows("RETURN point({longitude: 2.3522, latitude: 48.8566, height: 35})",
               {{"point({x: 2.3522, y: 48.8566, z: 35.0, crs: 'wgs-84-3d'})"}});
    expectRows("RETURN point({longitude: 2.3522, latitude: 48.8566, z: 35})",
               {{"point({x: 2.3522, y: 48.8566, z: 35.0, crs: 'wgs-84-3d'})"}});
}

TEST_F(PointPropertyTest, readsTheCRSANameOrAnSRIDGives) {
    expectRows("RETURN point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})",
               {{"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
    expectRows("RETURN point({x: 1, y: 2, crs: 'WGS-84'})", {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}});
    expectRows("RETURN point({x: 1, y: 2, srid: 4326})", {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}});
    expectRows("RETURN point({x: 1, y: 2, z: 3, srid: 9157})",
               {{"point({x: 1.0, y: 2.0, z: 3.0, crs: 'cartesian-3d'})"}});
    expectRows("RETURN point({longitude: 1, latitude: 2, height: 3, srid: 4979})",
               {{"point({x: 1.0, y: 2.0, z: 3.0, crs: 'wgs-84-3d'})"}});
}

TEST_F(PointPropertyTest, readsItsKeysWhateverTheirCase) {
    expectRows("RETURN point({X: 1, Y: 2})", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
    expectRows("RETURN point({Longitude: 1, LATITUDE: 2})", {{"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}});
}

TEST_F(PointPropertyTest, ignoresTheKeysItDoesNotKnow) {
    expectRows("RETURN point({x: 1, y: 2, name: 'a'})", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
}

TEST_F(PointPropertyTest, wrapsALongitudeIntoTheRange) {
    expectRows("RETURN point({longitude: 190, latitude: 0})", {{"point({x: -170.0, y: 0.0, crs: 'wgs-84'})"}});
    expectRows("RETURN point({longitude: -540, latitude: 0})", {{"point({x: -180.0, y: 0.0, crs: 'wgs-84'})"}});
    expectRows("RETURN point({longitude: 360, latitude: 0})", {{"point({x: 0.0, y: 0.0, crs: 'wgs-84'})"}});
}

TEST_F(PointPropertyTest, returnsNullForANullMapOrANullValue) {
    expectRows("RETURN point(null)", {{"null"}});
    expectRows("RETURN point({x: null, y: 2})", {{"null"}});
    expectRows("RETURN point({x: 1, y: 2, crs: null})", {{"null"}});
}

TEST_F(PointPropertyTest, rejectsALatitudePastAPole) {
    expectError("RETURN point({longitude: 0, latitude: 90.5})", "The latitude of a wgs-84 point lies in [-90, 90], not 90.5");
    expectError("RETURN point({x: 0, y: -91, crs: 'wgs-84'})", "lies in [-90, 90], not -91");
}

TEST_F(PointPropertyTest, rejectsANonFiniteCoordinate) {
    expectError("RETURN point({x: 1e308 * 10, y: 0})", "A point cannot have a non-finite coordinate");
}

TEST_F(PointPropertyTest, rejectsAMapNamingBothACRSAndAnSRID) {
    expectError("RETURN point({x: 1, y: 2, crs: 'cartesian', srid: 7203})", "point() takes a 'crs' or an 'srid', not both");
}

TEST_F(PointPropertyTest, rejectsAnUnknownCRS) {
    expectError("RETURN point({x: 1, y: 2, crs: 'mercator'})", "Unknown coordinate reference system: 'mercator'");
    expectError("RETURN point({x: 1, y: 2, srid: 3857})", "Unknown coordinate reference system SRID: 3857");
}

TEST_F(PointPropertyTest, rejectsAGeographicPointInACartesianCRS) {
    expectError("RETURN point({longitude: 1, latitude: 2, crs: 'cartesian'})",
                "A point given by longitude and latitude takes a geographic CRS, not 'cartesian'");
}

TEST_F(PointPropertyTest, rejectsCoordinatesTheCRSDoesNotHave) {
    expectError("RETURN point({x: 1, y: 2, crs: 'cartesian-3d'})", "A cartesian-3d point has 3 coordinates, not 2");
    expectError("RETURN point({x: 1, y: 2, z: 3, crs: 'wgs-84'})", "A wgs-84 point has 2 coordinates, not 3");
}

TEST_F(PointPropertyTest, rejectsAMapMissingACoordinate) {
    expectError("RETURN point({x: 1})", "point() needs 'x' and 'y', or 'longitude' and 'latitude'");
    expectError("RETURN point({latitude: 1, crs: 'wgs-84'})", "A wgs-84 point needs 'longitude' and 'latitude'");
}

TEST_F(PointPropertyTest, rejectsACoordinateThatIsNoNumber) {
    expectError("RETURN point({x: '1', y: 2})", "point() reads a number for 'x', not 'String'");
}

TEST_F(PointPropertyTest, rejectsAKeyGivenTwice) {
    expectError("RETURN point({x: 1, X: 2, y: 3})", "point() reads the key 'x' twice");
}

TEST_F(PointPropertyTest, buildsAPointFromAMapItReadsPerRow) {
    expectRows("UNWIND [{x: 1, y: 2}, {longitude: 3, latitude: 4}, null] AS m RETURN point(m)",
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"},
                {"point({x: 3.0, y: 4.0, crs: 'wgs-84'})"},
                {"null"}});
}

TEST_F(PointPropertyTest, rejectsAValueThatIsNoMap) {
    expectError("UNWIND [1] AS m RETURN point(m)", "point");
}

TEST_F(PointPropertyTest, writesAPointAsText) {
    expectRows("RETURN toString(point({x: 1, y: 2}))", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
    expectRows("UNWIND [point({x: 1, y: 2})] AS p RETURN toString(p)", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
}

TEST_F(PointPropertyTest, storesAndReadsBackAPoint) {
    write("CREATE (n:City {name: 'Paris', at: point({longitude: 2.3522, latitude: 48.8566})})");
    expectRows("MATCH (n:City) RETURN n.at", {{"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
}

TEST_F(PointPropertyTest, setsThePointOfAnExistingNode) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.home = point({x: 1, y: 2, z: 3})");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.home", {{"point({x: 1.0, y: 2.0, z: 3.0, crs: 'cartesian-3d'})"}});
}

TEST_F(PointPropertyTest, storesAPointOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[:MET {at: point({x: 1, y: 2})}]->(b)");

    expectRows("MATCH (:Person)-[e:MET]->(:Person) RETURN e.at", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
}

TEST_F(PointPropertyTest, readsNullFromANodeCarryingNoPoint) {
    write("CREATE (a:City {name: 'a', at: point({x: 1, y: 2})})");
    write("CREATE (b:City {name: 'b'})");

    expectRows("MATCH (n:City) RETURN n.name, n.at", {{"a", "point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"b", "null"}});
}

TEST_F(PointPropertyTest, holdsPointsOfDifferentCRSsInOneProperty) {
    createFourCities();
    expectRows("MATCH (n:City) RETURN n.name, n.at",
               {{"Paris", "point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"},
                {"London", "point({x: -0.1276, y: 51.5072, crs: 'wgs-84'})"},
                {"Grid", "point({x: 3.0, y: 4.0, crs: 'cartesian'})"},
                {"Tower", "point({x: 3.0, y: 4.0, z: 5.0, crs: 'cartesian-3d'})"}});
}

TEST_F(PointPropertyTest, filtersOnEquality) {
    createFourCities();
    expectRows("MATCH (n:City) WHERE n.at = point({x: 3, y: 4}) RETURN n.name", {{"Grid"}});
    expectRows("MATCH (n:City) WHERE n.at <> point({x: 3, y: 4}) RETURN n.name", {{"Paris"}, {"London"}, {"Tower"}});
}

TEST_F(PointPropertyTest, tellsPointsOfDifferentCRSsApart) {
    expectRows("RETURN point({x: 1, y: 2}) = point({x: 1, y: 2, crs: 'wgs-84'})", {{"false"}});
    expectRows("RETURN point({x: 1, y: 2}) = point({x: 1, y: 2, z: 0})", {{"false"}});
    expectRows("RETURN point({x: 1, y: 2}) = point({x: 1.0, y: 2.0})", {{"true"}});
}

TEST_F(PointPropertyTest, equatesAZeroToANegativeZero) {
    expectRows("RETURN point({x: -0.0, y: 0}) = point({x: 0.0, y: 0})", {{"true"}});
    expectRows("UNWIND [point({x: -0.0, y: 0}), point({x: 0.0, y: 0})] AS p RETURN DISTINCT p",
               {{"point({x: 0.0, y: 0.0, crs: 'cartesian'})"}});
}

TEST_F(PointPropertyTest, ordersNoPointBeforeAnother) {
    expectRows("RETURN point({x: 1, y: 2}) < point({x: 3, y: 4})", {{"null"}});
}

TEST_F(PointPropertyTest, ordersBySRIDThenByCoordinates) {
    createFourCities();
    write("CREATE (n:City {name: 'Origin', at: point({x: 0, y: 9})})");

    expectOrderedRows("MATCH (n:City) RETURN n.name ORDER BY n.at",
                      {{"London"}, {"Paris"}, {"Origin"}, {"Grid"}, {"Tower"}});
    expectOrderedRows("MATCH (n:City) RETURN n.name ORDER BY n.at DESC",
                      {{"Tower"}, {"Grid"}, {"Origin"}, {"Paris"}, {"London"}});
}

TEST_F(PointPropertyTest, sortsPointsAfterPathsAndBeforeDateTimes) {
    expectOrderedRows("UNWIND [datetime('2026-01-01'), point({x: 1, y: 2}), [1]] AS v RETURN v ORDER BY v",
                      {{"[1]"}, {"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"2026-01-01T00:00:00Z"}});
}

TEST_F(PointPropertyTest, countsAndCollectsPoints) {
    write("CREATE (a:City {name: 'a', at: point({x: 1, y: 2})})");
    write("CREATE (b:City {name: 'b', at: point({x: 1, y: 2})})");

    expectRows("MATCH (n:City) RETURN count(n.at), count(DISTINCT n.at)", {{"2", "1"}});
    expectRows("MATCH (n:City) RETURN collect(n.at)",
               {{"[point({x: 1.0, y: 2.0, crs: 'cartesian'}), point({x: 1.0, y: 2.0, crs: 'cartesian'})]"}});
}

TEST_F(PointPropertyTest, groupsByPoint) {
    write("CREATE (a:City {name: 'a', at: point({x: 1, y: 2})})");
    write("CREATE (b:City {name: 'b', at: point({x: 1, y: 2})})");
    write("CREATE (c:City {name: 'c', at: point({x: 1, y: 2, crs: 'wgs-84'})})");

    expectRows("MATCH (n:City) RETURN n.at, count(*)",
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})", "2"}, {"point({x: 1.0, y: 2.0, crs: 'wgs-84'})", "1"}});
}

TEST_F(PointPropertyTest, storesPointsInAList) {
    write("CREATE (n:City {name: 'a', route: [point({x: 1, y: 2}), point({longitude: 3, latitude: 4})]})");
    expectRows("MATCH (n:City) RETURN n.route",
               {{"[point({x: 1.0, y: 2.0, crs: 'cartesian'}), point({x: 3.0, y: 4.0, crs: 'wgs-84'})]"}});
}

TEST_F(PointPropertyTest, storesAPointInAMap) {
    write("CREATE (n:City {name: 'a', attrs: {at: point({x: 1, y: 2}), n: 1}})");
    expectRows("MATCH (n:City) RETURN n.attrs", {{"{at: point({x: 1.0, y: 2.0, crs: 'cartesian'}), n: 1}"}});
}

TEST_F(PointPropertyTest, findsAPointInAList) {
    expectRows("RETURN point({x: 1, y: 2}) IN [point({x: 3, y: 4}), point({x: 1, y: 2})]", {{"true"}});
    expectRows("RETURN point({x: 1, y: 2}) IN [point({x: 1, y: 2, crs: 'wgs-84'})]", {{"false"}});
}

TEST_F(PointPropertyTest, unwindsAListOfPoints) {
    expectRows("UNWIND [point({x: 3, y: 4}), point({x: 1, y: 2})] AS p RETURN p",
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"point({x: 3.0, y: 4.0, crs: 'cartesian'})"}});
}
