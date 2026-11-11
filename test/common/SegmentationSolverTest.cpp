#include <gtest/gtest.h>

#include <math.h>
#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <random>
#include <vector>

#include "SegmentationSolver.h"

using namespace db;

namespace {

using Boundary = SegmentationSolver::Boundary;

double segmentationCost(const std::vector<Boundary>& boundaries, const std::vector<size_t>& starts) {
    double cost = 0;
    for (size_t index = 0; index < starts.size(); index++) {
        const size_t start = starts[index];
        const size_t end = index + 1 < starts.size() ? starts[index + 1] : boundaries.size() - 1;
        const Boundary& first = boundaries[start];
        const Boundary& last = boundaries[end];
        cost += first._weight * (last._position - first._position) + first._startCost + last._endCost;
    }

    return cost;
}

void solve(const std::vector<Boundary>& boundaries, std::vector<size_t>& starts) {
    SegmentationSolver solver;
    for (const Boundary& boundary : boundaries) {
        solver.addBoundary(boundary);
    }

    solver.getSegmentStarts(starts);
}

// The least cost over every segmentation, trying each set of the cuttable boundaries
double bruteForceCost(const std::vector<Boundary>& boundaries) {
    const size_t inner = boundaries.size() - 2;
    double best = std::numeric_limits<double>::infinity();
    for (uint64_t cuts = 0; cuts < (uint64_t {1} << inner); cuts++) {
        std::vector<size_t> starts {0};
        bool allowed = true;
        for (size_t boundary = 1; boundary <= inner; boundary++) {
            if ((cuts >> (boundary - 1)) & 1) {
                allowed = allowed && boundaries[boundary]._cuttable;
                starts.push_back(boundary);
            }
        }

        if (allowed) {
            best = std::min(best, segmentationCost(boundaries, starts));
        }
    }

    return best;
}

// The least cost by the quadratic dynamic program the solver speeds up
double quadraticCost(const std::vector<Boundary>& boundaries) {
    std::vector<double> best(boundaries.size(), std::numeric_limits<double>::infinity());
    best[0] = 0;
    for (size_t end = 1; end < boundaries.size(); end++) {
        for (size_t start = 0; start < end; start++) {
            const bool startsASegment = start == 0 || boundaries[start]._cuttable;
            if (!startsASegment) {
                continue;
            }

            const Boundary& first = boundaries[start];
            const Boundary& last = boundaries[end];
            const double cost = best[start] + first._weight * (last._position - first._position) + first._startCost + last._endCost;
            best[end] = std::min(best[end], cost);
        }
    }

    return best.back();
}

// Weights shrink by a factor from @param leastFactor to 1, some of them exactly 1 and, with
// @param dropsToZero, some 0. Positions grow by a step some of which are 0, so lines tie on
// slope and queries tie on position.
void randomBoundaries(std::mt19937& random,
                      size_t count,
                      double leastFactor,
                      bool dropsToZero,
                      std::vector<Boundary>& boundaries) {
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    std::uniform_real_distribution<double> factor(leastFactor, 1.0);
    std::uniform_int_distribution<int> choice(0, 9);

    boundaries.clear();
    double weight = 1;
    double position = 0;
    for (size_t index = 0; index <= count; index++) {
        if (index > 0) {
            const int shape = choice(random);
            if (shape == 0 && dropsToZero) {
                weight = 0;
            } else if (shape >= 3) {
                weight *= factor(random);
            }

            position += choice(random) < 2 ? 0.0 : 20 * unit(random);
        }

        boundaries.push_back(Boundary {position, weight, unit(random), unit(random), choice(random) > 1});
    }
}

}

TEST(SegmentationSolverTest, keepsOneSegmentWhereNoCutPays) {
    const std::vector<Boundary> boundaries {{0, 1, 1, 0, true}, {1, 1, 1, 0.5, true}, {2, 1, 1, 0, true}};

    std::vector<size_t> starts;
    solve(boundaries, starts);

    EXPECT_EQ(starts, std::vector<size_t>({0}));
}

// One segment costs 1 * 11 + 1 = 12. Cutting at 1 costs 1 * 1 + 1 + 0.2 for the first and
// 0.1 * 10 + 1 for the second, 4.2 in all.
TEST(SegmentationSolverTest, cutsWhereTheWeightDrops) {
    const std::vector<Boundary> boundaries {{0, 1, 1, 0, true}, {1, 0.1, 1, 0.2, true}, {11, 0.1, 1, 0, true}};

    std::vector<size_t> starts;
    solve(boundaries, starts);

    EXPECT_EQ(starts, std::vector<size_t>({0, 1}));
}

TEST(SegmentationSolverTest, neverCutsWhereACutIsForbidden) {
    const std::vector<Boundary> boundaries {{0, 1, 1, 0, true}, {1, 0.1, 1, 0.2, false}, {11, 0.1, 1, 0, true}};

    std::vector<size_t> starts;
    solve(boundaries, starts);

    EXPECT_EQ(starts, std::vector<size_t>({0}));
}

TEST(SegmentationSolverTest, startsOverAfterClear) {
    SegmentationSolver solver;
    for (const Boundary& boundary : {Boundary {0, 1, 1, 0, true}, Boundary {1, 0.1, 1, 0.2, true}, Boundary {11, 0.1, 1, 0, true}}) {
        solver.addBoundary(boundary);
    }

    solver.clear();
    for (const Boundary& boundary : {Boundary {0, 1, 1, 0, true}, Boundary {1, 1, 1, 0.5, true}, Boundary {2, 1, 1, 0, true}}) {
        solver.addBoundary(boundary);
    }

    std::vector<size_t> starts;
    solver.getSegmentStarts(starts);

    EXPECT_EQ(starts, std::vector<size_t>({0}));
}

TEST(SegmentationSolverTest, costsWhatTheBestSegmentationCosts) {
    std::mt19937 random(7);
    std::vector<Boundary> boundaries;
    std::vector<size_t> starts;

    for (size_t trial = 0; trial < 2000; trial++) {
        randomBoundaries(random, 1 + trial % 12, 0.0, true, boundaries);
        solve(boundaries, starts);

        ASSERT_FALSE(starts.empty());
        EXPECT_EQ(starts.front(), 0u);
        for (size_t index = 1; index < starts.size(); index++) {
            EXPECT_LT(starts[index - 1], starts[index]);
            EXPECT_TRUE(boundaries[starts[index]]._cuttable);
        }

        const double expected = bruteForceCost(boundaries);
        EXPECT_NEAR(segmentationCost(boundaries, starts), expected, 1e-9 * (1 + expected)) << "trial " << trial;
    }
}

TEST(SegmentationSolverTest, costsWhatTheQuadraticProgramFindsOnLongSequences) {
    std::mt19937 random(11);
    std::vector<Boundary> boundaries;
    std::vector<size_t> starts;

    for (size_t trial = 0; trial < 20; trial++) {
        randomBoundaries(random, 2000, 0.99, false, boundaries);
        solve(boundaries, starts);

        const double expected = quadraticCost(boundaries);
        EXPECT_NEAR(segmentationCost(boundaries, starts), expected, 1e-9 * (1 + expected)) << "trial " << trial;
    }
}
