#include <gtest/gtest.h>

#include <stdint.h>
#include <string.h>

#include <functional>
#include <limits>
#include <string>

#include "metadata/Point.h"
#include "TuringException.h"

using namespace db;

namespace {

std::string formatted(const Point& value) {
    std::string out;
    Point::format(out, value);

    return out;
}

}

TEST(PointTest, writesTheCoordinatesItsCRSHas) {
    EXPECT_EQ(formatted(Point(PointCRS::Cartesian, 1.0, 2.5, 9.0)), "point({x: 1.0, y: 2.5, crs: 'cartesian'})");
    EXPECT_EQ(formatted(Point(PointCRS::Cartesian3D, 1.0, 2.5, 9.0)), "point({x: 1.0, y: 2.5, z: 9.0, crs: 'cartesian-3d'})");
    EXPECT_EQ(formatted(Point(PointCRS::WGS84, 2.3522, 48.8566, 0.0)), "point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})");
    EXPECT_EQ(formatted(Point(PointCRS::WGS84_3D, 2.3522, 48.8566, 35.0)), "point({x: 2.3522, y: 48.8566, z: 35.0, crs: 'wgs-84-3d'})");
}

TEST(PointTest, holdsAZOfZeroIn2D) {
    const Point point(PointCRS::Cartesian, 1.0, 2.0, 9.0);

    EXPECT_EQ(point.getZ(), 0.0);
    EXPECT_EQ(point, Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));
}

TEST(PointTest, holdsNoNegativeZero) {
    const Point negative(PointCRS::Cartesian3D, -0.0, -0.0, -0.0);
    const Point positive(PointCRS::Cartesian3D, 0.0, 0.0, 0.0);

    EXPECT_EQ(memcmp(&negative, &positive, sizeof(Point)), 0);
    EXPECT_EQ(std::hash<Point> {}(negative), std::hash<Point> {}(positive));
}

TEST(PointTest, wrapsALongitudeIntoTheRange) {
    EXPECT_EQ(Point(PointCRS::WGS84, 190.0, 0.0, 0.0).getX(), -170.0);
    EXPECT_EQ(Point(PointCRS::WGS84, -190.0, 0.0, 0.0).getX(), 170.0);
    EXPECT_EQ(Point(PointCRS::WGS84, 180.0, 0.0, 0.0).getX(), 180.0);
    EXPECT_EQ(Point(PointCRS::WGS84, -180.0, 0.0, 0.0).getX(), -180.0);
    EXPECT_EQ(Point(PointCRS::WGS84, 720.0, 0.0, 0.0).getX(), 0.0);
    EXPECT_EQ(Point(PointCRS::Cartesian, 720.0, 0.0, 0.0).getX(), 720.0);
}

TEST(PointTest, rejectsALatitudePastAPole) {
    EXPECT_THROW(Point(PointCRS::WGS84, 0.0, 90.5, 0.0), TuringException);
    EXPECT_THROW(Point(PointCRS::WGS84_3D, 0.0, -90.5, 0.0), TuringException);
    EXPECT_NO_THROW(Point(PointCRS::WGS84, 0.0, 90.0, 0.0));
    EXPECT_NO_THROW(Point(PointCRS::Cartesian, 0.0, 90.5, 0.0));
}

TEST(PointTest, rejectsANonFiniteCoordinate) {
    EXPECT_THROW(Point(PointCRS::Cartesian, std::numeric_limits<double>::infinity(), 0.0, 0.0), TuringException);
    EXPECT_THROW(Point(PointCRS::Cartesian3D, 0.0, 0.0, std::numeric_limits<double>::quiet_NaN()), TuringException);
}

TEST(PointTest, ordersBySRIDThenByCoordinates) {
    const Point wgs84(PointCRS::WGS84, 100.0, 0.0, 0.0);
    const Point wgs84With3D(PointCRS::WGS84_3D, -100.0, 0.0, 0.0);
    const Point cartesian(PointCRS::Cartesian, -100.0, 0.0, 0.0);
    const Point cartesian3D(PointCRS::Cartesian3D, -100.0, 0.0, 0.0);

    EXPECT_LT(wgs84, wgs84With3D);
    EXPECT_LT(wgs84With3D, cartesian);
    EXPECT_LT(cartesian, cartesian3D);

    EXPECT_LT(Point(PointCRS::Cartesian, 1.0, 9.0, 0.0), Point(PointCRS::Cartesian, 2.0, 0.0, 0.0));
    EXPECT_LT(Point(PointCRS::Cartesian, 1.0, 1.0, 0.0), Point(PointCRS::Cartesian, 1.0, 2.0, 0.0));
    EXPECT_LT(Point(PointCRS::Cartesian3D, 1.0, 1.0, 1.0), Point(PointCRS::Cartesian3D, 1.0, 1.0, 2.0));
}

TEST(PointTest, namesTheCRSAndItsSRID) {
    EXPECT_EQ(Point::getCRSName(PointCRS::Cartesian), "cartesian");
    EXPECT_EQ(Point::getSRID(PointCRS::Cartesian), 7203);
    EXPECT_EQ(Point::getCRSName(PointCRS::Cartesian3D), "cartesian-3d");
    EXPECT_EQ(Point::getSRID(PointCRS::Cartesian3D), 9157);
    EXPECT_EQ(Point::getCRSName(PointCRS::WGS84), "wgs-84");
    EXPECT_EQ(Point::getSRID(PointCRS::WGS84), 4326);
    EXPECT_EQ(Point::getCRSName(PointCRS::WGS84_3D), "wgs-84-3d");
    EXPECT_EQ(Point::getSRID(PointCRS::WGS84_3D), 4979);
}

TEST(PointTest, keepsTheOrdinalsOfItsCRSs) {
    EXPECT_EQ(static_cast<uint8_t>(PointCRS::Cartesian), 0);
    EXPECT_EQ(static_cast<uint8_t>(PointCRS::Cartesian3D), 1);
    EXPECT_EQ(static_cast<uint8_t>(PointCRS::WGS84), 2);
    EXPECT_EQ(static_cast<uint8_t>(PointCRS::WGS84_3D), 3);
}
