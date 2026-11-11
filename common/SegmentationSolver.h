#pragma once

#include <stddef.h>

#include <vector>

namespace db {

// The cheapest cut of boundaries 0..n into segments [start, end), a segment costing
// weight(start) * (position(end) - position(start)) + startCost(start) + endCost(end), with weight
// never rising and position never falling. Each start is a line in the end's position, and the
// cheapest start for every end lies on their lower envelope (the convex hull trick).
class SegmentationSolver {
public:
    struct Boundary {
        double _position {0};
        double _weight {0};
        double _startCost {0};
        double _endCost {0};
        bool _cuttable {true};
    };

    SegmentationSolver();
    ~SegmentationSolver();

    // Forgets the boundaries, keeping the memory for the next sequence
    void clear();

    // A segment starts at the first boundary and one ends at the last. In between, a segment
    // may start where `_cuttable` is set.
    void addBoundary(const Boundary& boundary);

    // The boundary each segment of the cheapest segmentation starts at, in order
    void getSegmentStarts(std::vector<size_t>& starts) const;

private:
    // A segment starting at `_boundary`: its cost, plus the cheapest segmentation before it, is
    // `_slope * position + _intercept` for an end at `position`
    struct StartLine {
        double _slope {0};
        double _intercept {0};
        size_t _boundary {0};
    };

    std::vector<StartLine> _lines;
    size_t _firstLiveLine {0};
    std::vector<size_t> _lastSegmentStarts;

    void addLine(const StartLine& line);
    const StartLine& findCheapestLine(double position);

    static double evaluate(const StartLine& line, double position);
};

}
