#include <gtest/gtest.h>

#include <math.h>

#include <numbers>
#include <optional>

#include "metadata/Point.h"

using namespace db;

namespace {

constexpr double EARTH_MEAN_RADIUS_METRES = 6371008.8;
constexpr double HALF_THE_EARTH_METRES = std::numbers::pi * EARTH_MEAN_RADIUS_METRES;

}

TEST(PointDistanceTest, measuresHalfTheEarthBetweenAntipodes) {
    const std::optional<double> distance = Point::distance(Point(PointCRS::WGS84, -178.0, 8.0, 0.0),
                                                           Point(PointCRS::WGS84, 2.0, -8.0, 0.0));

    ASSERT_TRUE(distance.has_value());
    EXPECT_NEAR(*distance, HALF_THE_EARTH_METRES, 1.0);
}

TEST(PointDistanceTest, measuresHalfTheEarthBetweenEveryPairOfIntegerAntipodes) {
    for (int latitude = -89; latitude <= 89; latitude++) {
        for (int longitude = -179; longitude <= 0; longitude++) {
            const Point point(PointCRS::WGS84, longitude, latitude, 0.0);
            const Point antipode(PointCRS::WGS84, longitude + 180, -latitude, 0.0);

            const std::optional<double> distance = Point::distance(point, antipode);

            ASSERT_TRUE(distance.has_value());
            ASSERT_NEAR(*distance, HALF_THE_EARTH_METRES, 1.0) << "longitude " << longitude << ", latitude " << latitude;
        }
    }
}

TEST(PointDistanceTest, measuresHalfTheEarthBetween3DAntipodesAtSeaLevel) {
    const std::optional<double> distance = Point::distance(Point(PointCRS::WGS84_3D, -178.0, 8.0, 0.0),
                                                           Point(PointCRS::WGS84_3D, 2.0, -8.0, 0.0));

    ASSERT_TRUE(distance.has_value());
    EXPECT_NEAR(*distance, HALF_THE_EARTH_METRES, 1.0);
}
