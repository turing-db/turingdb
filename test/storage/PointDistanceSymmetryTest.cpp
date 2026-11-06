#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "metadata/Point.h"

using namespace db;

namespace {

void geographicPoints(PointCRS crs, std::vector<Point>& points) {
    for (int latitude = -85; latitude <= 85; latitude += 17) {
        for (int longitude = -175; longitude <= 175; longitude += 23) {
            points.emplace_back(crs, longitude + 0.35, latitude + 0.85, 120.5);
        }
    }
}

}

TEST(PointDistanceSymmetryTest, measuresZeroFromAGeographicPointToItself) {
    for (const PointCRS crs : {PointCRS::WGS84, PointCRS::WGS84_3D}) {
        std::vector<Point> points;
        geographicPoints(crs, points);

        for (const Point& point : points) {
            const std::optional<double> distance = Point::distance(point, point);

            ASSERT_TRUE(distance.has_value());
            ASSERT_EQ(*distance, 0.0) << point.getX() << ", " << point.getY();
        }
    }
}

TEST(PointDistanceSymmetryTest, measuresTheSameDistanceEitherWay) {
    for (const PointCRS crs : {PointCRS::WGS84, PointCRS::WGS84_3D}) {
        std::vector<Point> points;
        geographicPoints(crs, points);

        for (size_t lhs = 0; lhs < points.size(); lhs++) {
            for (size_t rhs = lhs + 1; rhs < points.size(); rhs++) {
                const std::optional<double> there = Point::distance(points[lhs], points[rhs]);
                const std::optional<double> back = Point::distance(points[rhs], points[lhs]);

                ASSERT_TRUE(there.has_value() && back.has_value());
                ASSERT_EQ(*there, *back) << lhs << " and " << rhs;
            }
        }
    }
}
