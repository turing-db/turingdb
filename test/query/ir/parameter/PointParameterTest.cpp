#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "metadata/Point.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

class PointParameterTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(PointParameterTest, returnsAPointParameterInEachCRS) {
    ParameterMap parameters;
    parameters.insert("c").setPoint(Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));
    parameters.insert("c3").setPoint(Point(PointCRS::Cartesian3D, 1.5, -2.0, 3.0));
    parameters.insert("g").setPoint(Point(PointCRS::WGS84, 2.3522, 48.8566, 0.0));
    parameters.insert("g3").setPoint(Point(PointCRS::WGS84_3D, 2.3522, 48.8566, 35.0));

    expectRows("RETURN $c, $c3, $g, $g3",
               parameters,
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})",
                 "point({x: 1.5, y: -2.0, z: 3.0, crs: 'cartesian-3d'})",
                 "point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})",
                 "point({x: 2.3522, y: 48.8566, z: 35.0, crs: 'wgs-84-3d'})"}});
}

TEST_F(PointParameterTest, equalsThePointLiteralItHolds) {
    ParameterMap parameters;
    parameters.insert("c").setPoint(Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));
    parameters.insert("g3").setPoint(Point(PointCRS::WGS84_3D, 2.3522, 48.8566, 35.0));

    expectRows("RETURN $c = point({x: 1.0, y: 2.0}), $g3 = point({longitude: 2.3522, latitude: 48.8566, height: 35.0}), $c = $g3",
               parameters,
               {{"true", "true", "false"}});
}

TEST_F(PointParameterTest, readsTheComponentsOfAPointParameter) {
    ParameterMap parameters;
    parameters.insert("pt").setPoint(Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));
    parameters.insert("g3").setPoint(Point(PointCRS::WGS84_3D, 2.5, 48.5, 35.0));

    expectRows("RETURN $pt.x, $pt.y, $pt.crs, $pt.srid", parameters, {{"1", "2", "cartesian", "7203"}});
    expectRows("RETURN $g3.longitude, $g3.latitude, $g3.height, $g3.srid", parameters, {{"2.5", "48.5", "35", "4979"}});
}

TEST_F(PointParameterTest, measuresADistanceFromAPointParameter) {
    ParameterMap parameters;
    parameters.insert("pt").setPoint(Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));
    parameters.insert("other").setPoint(Point(PointCRS::Cartesian, 4.0, 6.0, 0.0));

    expectRows("RETURN point.distance($pt, point({x: 4.0, y: 6.0})), point.distance($pt, $other)", parameters, {{"5", "5"}});
}

TEST_F(PointParameterTest, writesAPointParameterToAProperty) {
    ParameterMap parameters;
    parameters.insert("pt").setPoint(Point(PointCRS::WGS84, 2.3522, 48.8566, 0.0));

    runWrite("CREATE (:Place {name: 'Paris', loc: $pt})", parameters);

    StringRowSink sink;
    runQuery("MATCH (n:Place) RETURN n.name, n.loc", sink);

    const Rows expected {{"Paris", "point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(PointParameterTest, setsAPointPropertyFromAParameter) {
    runWrite("CREATE (:Place {name: 'Grid', loc: point({x: 0.0, y: 0.0})})");

    ParameterMap parameters;
    parameters.insert("pt").setPoint(Point(PointCRS::Cartesian3D, 3.0, 4.0, 12.0));

    runWrite("MATCH (n:Place {name: 'Grid'}) SET n.loc = $pt", parameters);

    StringRowSink sink;
    runQuery("MATCH (n:Place) RETURN n.loc", sink);

    const Rows expected {{"point({x: 3.0, y: 4.0, z: 12.0, crs: 'cartesian-3d'})"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(PointParameterTest, matchesAPointPropertyAgainstAParameter) {
    runWrite("CREATE (:Place {name: 'Paris', loc: point({longitude: 2.3522, latitude: 48.8566})})");
    runWrite("CREATE (:Place {name: 'London', loc: point({longitude: -0.1276, latitude: 51.5072})})");
    runWrite("CREATE (:Place {name: 'Grid', loc: point({x: 2.3522, y: 48.8566})})");

    ParameterMap parameters;
    parameters.insert("pt").setPoint(Point(PointCRS::WGS84, 2.3522, 48.8566, 0.0));

    expectRows("MATCH (n {loc: $pt}) RETURN n.name", parameters, {{"Paris"}});
    expectRows("MATCH (n:Place {loc: $pt}) RETURN n.name", parameters, {{"Paris"}});
    expectRows("MATCH (n:Place) WHERE n.loc = $pt RETURN n.name", parameters, {{"Paris"}});
    expectRows("MATCH (n:Place) WHERE n.loc <> $pt RETURN n.name ORDER BY n.name", parameters, {{"Grid"}, {"London"}});
}

TEST_F(PointParameterTest, readsAPointInsideAListOrAMapParameter) {
    ParameterMap parameters;
    ParameterValue::List& points = parameters.insert("pts").setList();
    points.emplace_back().setPoint(Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));
    points.emplace_back().setPoint(Point(PointCRS::Cartesian, 4.0, 6.0, 0.0));
    points.emplace_back().setNull();

    ParameterValue::Map& stop = parameters.insert("stop").setMap();
    stop["name"].setString("Paris");
    stop["at"].setPoint(Point(PointCRS::WGS84, 2.3522, 48.8566, 0.0));

    expectRows("UNWIND $pts AS p RETURN p",
               parameters,
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"point({x: 4.0, y: 6.0, crs: 'cartesian'})"}, {"null"}});
    expectRows("RETURN point.distance($pts[0], $pts[1]), $pts[1].y", parameters, {{"5", "6"}});
    expectRows("RETURN $stop.at, $stop.at.latitude", parameters, {{"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})", "48.8566"}});
}

TEST_F(PointParameterTest, writesThePointsOfAMapParameter) {
    runWrite("CREATE (:Place {name: 'Tower'})");

    ParameterMap parameters;
    ParameterValue::Map& properties = parameters.insert("props").setMap();
    properties["loc"].setPoint(Point(PointCRS::Cartesian3D, 3.0, 4.0, 5.0));
    properties["base"].setPoint(Point(PointCRS::Cartesian, 3.0, 4.0, 0.0));

    runWrite("MATCH (n:Place) SET n += $props", parameters);

    StringRowSink sink;
    runQuery("MATCH (n:Place) RETURN n.name, n.loc, point.distance(n.loc, point({x: 0, y: 0, z: 0})), n.base", sink);

    const Rows expected {{"Tower",
                          "point({x: 3.0, y: 4.0, z: 5.0, crs: 'cartesian-3d'})",
                          "7.0710678118654755",
                          "point({x: 3.0, y: 4.0, crs: 'cartesian'})"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(PointParameterTest, refusesAPointParameterWhereAPropertyHoldsAnotherType) {
    ParameterMap parameters;
    parameters.insert("pt").setPoint(Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));

    runWriteExpectingError("CREATE (:Place {name: $pt})", parameters, "incompatible");
}

TEST_F(PointParameterTest, mergesOnAPointParameter) {
    ParameterMap parameters;
    parameters.insert("pt").setPoint(Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));

    runWrite("MERGE (:Place {loc: $pt})", parameters);
    runWrite("MERGE (:Place {loc: $pt})", parameters);

    StringRowSink sink;
    runQuery("MATCH (n:Place) RETURN count(n), collect(n.loc)", sink);

    const Rows expected {{"1", "point({x: 1.0, y: 2.0, crs: 'cartesian'})"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(PointParameterTest, createsANodeForEachPointOfAListParameter) {
    ParameterMap parameters;
    ParameterValue::List& points = parameters.insert("pts").setList();
    points.emplace_back().setPoint(Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));
    points.emplace_back().setPoint(Point(PointCRS::Cartesian, 4.0, 6.0, 0.0));

    runWrite("UNWIND $pts AS p CREATE (:Place {loc: p})", parameters);

    expectRows("MATCH (n:Place) WHERE n.loc IN $pts RETURN n.loc.x ORDER BY n.loc.x", parameters, {{"1"}, {"4"}});
    expectRows("MATCH (n:Place) WHERE n.loc = $pts[1] RETURN n.loc.y", parameters, {{"6"}});
}

TEST_F(PointParameterTest, findsAPointInABoxGivenByParameters) {
    ParameterMap parameters;
    parameters.insert("lowerLeft").setPoint(Point(PointCRS::WGS84, 2.0, 48.0, 0.0));
    parameters.insert("upperRight").setPoint(Point(PointCRS::WGS84, 3.0, 49.0, 0.0));

    expectRows("RETURN point.withinBBox(point({longitude: 2.3522, latitude: 48.8566}), $lowerLeft, $upperRight), "
               "point.withinBBox(point({longitude: -0.1276, latitude: 51.5072}), $lowerLeft, $upperRight)",
               parameters,
               {{"true", "false"}});
}
