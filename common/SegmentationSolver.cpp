#include "SegmentationSolver.h"

#include <algorithm>

using namespace db;

SegmentationSolver::SegmentationSolver()
{
}

SegmentationSolver::~SegmentationSolver() {
}

void SegmentationSolver::clear() {
    _lines.clear();
    _firstLiveLine = 0;
    _lastSegmentStarts.clear();
}

void SegmentationSolver::addBoundary(const Boundary& boundary) {
    const size_t index = _lastSegmentStarts.size();

    double cheapestCost = 0;
    if (index == 0) {
        _lastSegmentStarts.push_back(0);
    } else {
        const StartLine& cheapest = findCheapestLine(boundary._position);
        cheapestCost = evaluate(cheapest, boundary._position) + boundary._endCost;
        _lastSegmentStarts.push_back(cheapest._boundary);

        if (!boundary._cuttable) {
            return;
        }
    }

    const double intercept = cheapestCost + boundary._startCost - boundary._weight * boundary._position;
    addLine(StartLine {boundary._weight, intercept, index});
}

void SegmentationSolver::getSegmentStarts(std::vector<size_t>& starts) const {
    starts.clear();
    if (_lastSegmentStarts.empty()) {
        return;
    }

    for (size_t end = _lastSegmentStarts.size() - 1; end > 0; end = _lastSegmentStarts[end]) {
        starts.push_back(_lastSegmentStarts[end]);
    }

    std::reverse(starts.begin(), starts.end());
}

// The lines arrive by falling slope, so the new one is the cheapest for the furthest ends. A
// line before it never is again once the new one undercuts it where it would itself start to
// undercut the line before it.
void SegmentationSolver::addLine(const StartLine& line) {
    while (_lines.size() > _firstLiveLine) {
        const StartLine& last = _lines.back();
        if (last._slope == line._slope) {
            if (last._intercept <= line._intercept) {
                return;
            }

            _lines.pop_back();
            continue;
        } else if (_lines.size() - _firstLiveLine < 2) {
            break;
        }

        const StartLine& beforeLast = _lines[_lines.size() - 2];
        const double newCrossing = (line._intercept - beforeLast._intercept) * (beforeLast._slope - last._slope);
        const double lastCrossing = (last._intercept - beforeLast._intercept) * (beforeLast._slope - line._slope);
        if (newCrossing > lastCrossing) {
            break;
        }

        _lines.pop_back();
    }

    _lines.push_back(line);
}

// Positions only grow, so a line a later one undercuts here stays undercut
const SegmentationSolver::StartLine& SegmentationSolver::findCheapestLine(double position) {
    while (_firstLiveLine + 1 < _lines.size()
           && evaluate(_lines[_firstLiveLine + 1], position) <= evaluate(_lines[_firstLiveLine], position)) {
        _firstLiveLine++;
    }

    return _lines[_firstLiveLine];
}

double SegmentationSolver::evaluate(const StartLine& line, double position) {
    return line._slope * position + line._intercept;
}
