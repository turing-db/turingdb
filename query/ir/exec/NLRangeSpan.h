#pragma once

#include <stdint.h>
#include <algorithm>

#include "metadata/PropertyType.h"

#include "NLProgram.h"

#include "IRException.h"
#include "IRRange.h"

namespace db {

// The integers one row's range bounds span. A row with a null bound spans no list at all,
// and one whose step walks away from its end spans the empty list.
class NLRangeSpan {
public:
    // Returns false where a bound of @param row holds no value
    bool read(const NLRangeBound& start, const NLRangeBound& end, const NLRangeBound& step, size_t row) {
        const std::optional<types::Int64::Primitive> from = start._read(start._column, row);
        const std::optional<types::Int64::Primitive> to = end._read(end._column, row);
        const std::optional<types::Int64::Primitive> by =
            step._column ? step._read(step._column, row) : std::optional<types::Int64::Primitive> {1};

        const bool boundsPresent = from.has_value() && to.has_value() && by.has_value();
        if (!boundsPresent) {
            return false;
        }

        if (*by == 0) {
            throw IRException("range() cannot count by a step of 0");
        }

        _windowFirst = 0;
        _reversed = false;
        _ascending = *by > 0;
        _empty = _ascending ? *from > *to : *from < *to;
        _first = static_cast<uint64_t>(*from);
        _stride = _ascending ? static_cast<uint64_t>(*by) : 0 - static_cast<uint64_t>(*by);
        _steps = countRangeSteps(*from, *to, *by);

        return true;
    }

    // Narrows the span to the run a slice of its list keeps, as runListSlice cuts it, and
    // counts that run out backwards where @param window is reversed. Returns false where a
    // slice bound of @param row holds no value.
    bool keepWindow(const NLRangeWindowBounds& window, size_t row) {
        const NLRangeBound& from = window._from;
        const NLRangeBound& to = window._to;

        const std::optional<types::Int64::Primitive> lower =
            from._column ? from._read(from._column, row) : std::optional<types::Int64::Primitive> {0};
        const std::optional<types::Int64::Primitive> upper =
            to._column ? to._read(to._column, row) : std::optional<types::Int64::Primitive> {};

        const bool boundsPresent = lower.has_value() && (!to._column || upper.has_value());
        if (!boundsPresent) {
            return false;
        }

        _reversed = window._reversed;

        if (_empty) {
            return true;
        }

        // The list holds _steps + 1 integers, up to 2^64, so the run is located by its first
        // and last positions, and a bound counting from the end by how far it stops short of it
        uint64_t first = 0;
        if (*lower >= 0) {
            first = static_cast<uint64_t>(*lower);
        } else {
            const uint64_t shortOfLast = static_cast<uint64_t>(-(*lower + 1));
            first = shortOfLast >= _steps ? 0 : _steps - shortOfLast;
        }

        const types::Int64::Primitive upperBound = upper.value_or(0);

        uint64_t last = 0;
        bool upperKeepsAny = true;
        if (!to._column) {
            last = _steps;
        } else if (upperBound > 0) {
            last = std::min(static_cast<uint64_t>(upperBound) - 1, _steps);
        } else if (upperBound == 0) {
            upperKeepsAny = false;
        } else {
            const uint64_t shortOfLast = static_cast<uint64_t>(-(upperBound + 1));
            upperKeepsAny = shortOfLast < _steps;
            last = upperKeepsAny ? _steps - shortOfLast - 1 : 0;
        }

        _empty = !upperKeepsAny || first > last;
        _windowFirst = first;
        _steps = _empty ? 0 : last - first;

        return true;
    }

    bool isEmpty() const { return _empty; }
    uint64_t getSteps() const { return _steps; }

    // The stride can be wider than an int64 holds, so every integer is offset from the
    // first unsigned
    types::Int64::Primitive at(uint64_t position) const {
        const uint64_t listPosition = _reversed ? _windowFirst + _steps - position : _windowFirst + position;
        const uint64_t offset = listPosition * _stride;
        return static_cast<types::Int64::Primitive>(_ascending ? _first + offset : _first - offset);
    }

private:
    uint64_t _first {0};
    uint64_t _stride {0};
    uint64_t _steps {0};
    uint64_t _windowFirst {0};
    bool _ascending {true};
    bool _empty {true};
    bool _reversed {false};
};

}
