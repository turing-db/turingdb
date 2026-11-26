#pragma once

#include <stdint.h>
#include <functional>
#include <limits>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnOperators.h"
#include "CellUtils.h"
#include "QueryTypeName.h"
#include "metadata/PropertyType.h"

#include "NLRuntimeProperty.h"
#include "NLWrittenValues.h"
#include "NLSortOrder.h"
#include "NLTaggedCells.h"

#include "IRException.h"
#include "BioAssert.h"

namespace db {

// Add two aggregate values with defined overflow. A signed integer sum wraps in
// two's complement (matching Cypher's Java-long integer sum) rather than invoking
// C++ signed-overflow undefined behavior; unsigned integers already wrap by the
// language, and doubles use the built-in +.
template <typename Primitive>
Primitive numericAdd(Primitive accumulator, Primitive value) {
    if constexpr (std::is_integral_v<Primitive> && std::is_signed_v<Primitive>) {
        return wrappingArithmetic<std::plus<>>(accumulator, value);
    } else {
        return accumulator + value;
    }
}

// A number sums as an f64 and a point keeps its running mean. A duration's total is the
// state's 128-bit one.
template <typename Primitive>
using AverageTotal = std::conditional_t<std::is_same_v<Primitive, types::Duration::Primitive>
                                            || std::is_same_v<Primitive, types::Point::Primitive>,
                                        Primitive,
                                        types::Double::Primitive>;

// @param count is how many values @param total already holds, which a running mean needs
template <typename Primitive>
AverageTotal<Primitive> addToAverageTotal(AverageTotal<Primitive> total, Primitive value, uint64_t count) {
    if constexpr (std::is_same_v<Primitive, types::Point::Primitive>) {
        return Point::addToMean(total, count, value);
    } else {
        return total + static_cast<types::Double::Primitive>(value);
    }
}

inline types::Double::Primitive averageOf(types::Double::Primitive total, uint64_t count) {
    return total / static_cast<types::Double::Primitive>(count);
}

// The mean of durations is within their range, so it is a duration. A mean finer than a
// microsecond truncates toward zero, as a duration divided by an integer does.
inline types::Duration::Primitive averageOf(__int128 total, uint64_t count) {
    return types::Duration::Primitive(static_cast<int64_t>(total / static_cast<__int128>(count)));
}

// A sum of durations is a 128-bit count of microseconds until it is emitted, so a total
// that passes the range of a duration and comes back answers in every row order
inline types::Duration::Primitive durationOfTotal(__int128 total) {
    const bool outsideTheRange = total > std::numeric_limits<int64_t>::max()
                              || total < std::numeric_limits<int64_t>::min();

    if (outsideTheRange) {
        throw IRException("Duration overflow");
    }

    return types::Duration::Primitive(static_cast<int64_t>(total));
}

inline types::Point::Primitive averageOf(types::Point::Primitive mean, uint64_t count) {
    return mean;
}

inline std::optional<CellView> presentRow(const ColumnVector<CellView>* column, size_t row) {
    return presentCell(column->getRaw()[row]);
}

template <typename Primitive>
std::optional<Primitive> presentRow(const ColumnOptVector<Primitive>* column, size_t row) {
    return operandRow(column, row);
}

template <typename Primitive, bool IsMax>
bool replacesExtremum(const Primitive& current, const Primitive& value) {
    if constexpr (IsMax) {
        return sortsBefore(current, value);
    } else {
        return sortsBefore(value, current);
    }
}

// A standard deviation keeps the mean of the values folded so far and the sum of their
// squared deviations from it. Welford's update keeps both accurate for values far from 0,
// where a sum of squares minus a squared sum cancels to noise. The count keeps the two
// doubles apart: side by side, GCC packs them in one register and serializes the update.
struct NLDeviationState {
    double _mean {0.0};
    uint64_t _count {0};
    double _squaredDeviations {0.0};
};

template <typename Primitive>
double numberOf(const Primitive& value) {
    return static_cast<double>(value);
}

inline double numberOf(const CellView element) {
    return taggedNumericValue(element);
}

inline void foldDeviation(double value, double& mean, double& squaredDeviations, uint64_t& count) {
    count++;

    const double distanceToOldMean = value - mean;
    mean += distanceToOldMean / static_cast<double>(count);
    squaredDeviations += distanceToOldMean * (value - mean);
}

constexpr std::byte INTEGER_ZERO_CELL[sizeof(CellTag) + sizeof(types::Int64::Primitive)] {
    static_cast<std::byte>(CellTag::Int),
};

// The running sum of tagged cells, as Cypher adds them: an integer while every number
// summed is one, a float from the first float on, a duration over durations, added into a
// 128-bit total of microseconds, and the running mean of the points an average reads. A null
// sum has summed nothing yet. Integers wrap as a typed column's sum does.
class NLTaggedSum {
public:
    void add(CellView value) {
        const CellTag tag = value.getTag();

        switch (tag) {
            case CellTag::Int:
                addInteger(tag, value.getAs<types::Int64::Primitive>());
            break;

            case CellTag::UInt:
                addInteger(tag, static_cast<types::Int64::Primitive>(value.getAs<types::UInt64::Primitive>()));
            break;

            case CellTag::Double:
                addReal(tag, value.getAs<types::Double::Primitive>());
            break;

            case CellTag::Duration:
                addDuration(value.getAs<types::Duration::Primitive>());
            break;

            default:
                throw IRException(fmt::format("sum() and avg() read numbers or durations, not '{}'", queryTypeName(tag)));
            break;
        }
    }

    // An average reads every number as a float, as it does over a typed column. @param count
    // is how many cells the average already holds, which the running mean of points needs.
    void addToAverage(CellView value, uint64_t count) {
        const CellTag tag = value.getTag();
        const bool isNumber = tag == CellTag::Int || tag == CellTag::UInt || tag == CellTag::Double;

        if (isNumber) {
            addReal(tag, taggedNumericValue(value));
        } else if (tag == CellTag::Point) {
            addPoint(value.getAs<types::Point::Primitive>(), count);
        } else {
            add(value);
        }
    }

    CellView sum(QueryListBuffer& cells) const {
        if (_tag == CellTag::Null) {
            return CellView {INTEGER_ZERO_CELL};
        } else if (_tag == CellTag::Duration) {
            return stagedCell(&cells, durationOfTotal(_durationTotal));
        } else if (_tag == CellTag::Double) {
            return stagedCell(&cells, _real);
        }

        return stagedCell(&cells, _integer);
    }

    CellView average(uint64_t count, QueryListBuffer& cells) const {
        if (_tag == CellTag::Null) {
            return CellView::nullElement();
        } else if (_tag == CellTag::Duration) {
            return stagedCell(&cells, averageOf(_durationTotal, count));
        } else if (_tag == CellTag::Point) {
            return stagedCell(&cells, _point);
        }

        bioassert(_tag == CellTag::Double, "An average sums its numbers as floats");

        return stagedCell(&cells, averageOf(_real, count));
    }

private:
    CellTag _tag {CellTag::Null};
    types::Int64::Primitive _integer {0};
    types::Double::Primitive _real {0.0};
    __int128 _durationTotal {0};
    types::Point::Primitive _point {};

    // Numbers sum together, and a duration or a point only with its own kind
    static CellTag sumKind(CellTag tag) {
        const bool sumsAlone = tag == CellTag::Duration || tag == CellTag::Point;
        return sumsAlone ? tag : CellTag::Double;
    }

    void throwIfMixing(CellTag tag) const {
        const bool summedAny = _tag != CellTag::Null;
        const bool mixes = sumKind(_tag) != sumKind(tag);

        if (summedAny && mixes) {
            throw IRException(fmt::format("sum() and avg() cannot add '{}' to a sum of '{}'",
                                          queryTypeName(tag),
                                          queryTypeName(_tag)));
        }
    }

    void addInteger(CellTag tag, types::Int64::Primitive value) {
        throwIfMixing(tag);

        if (_tag == CellTag::Double) {
            _real += static_cast<types::Double::Primitive>(value);
        } else {
            _tag = CellTag::Int;
            _integer = numericAdd(_integer, value);
        }
    }

    void addReal(CellTag tag, types::Double::Primitive value) {
        throwIfMixing(tag);

        if (_tag != CellTag::Double) {
            _tag = CellTag::Double;
            _real = static_cast<types::Double::Primitive>(_integer);
        }

        _real += value;
    }

    void addDuration(types::Duration::Primitive value) {
        throwIfMixing(CellTag::Duration);

        _tag = CellTag::Duration;
        _durationTotal += value.getMicroseconds();
    }

    void addPoint(types::Point::Primitive value, uint64_t count) {
        throwIfMixing(CellTag::Point);

        _tag = CellTag::Point;
        _point = Point::addToMean(_point, count, value);
    }
};

// The value a group keeps - its min, max or first - null until the group sees one. A string,
// list or map keeps the store's copy of what it views.
template <typename Primitive>
class NLKeptValue {
public:
    bool isKept() const { return _value.has_value(); }
    const Primitive& getValue() const { return *_value; }

    void keep(const Primitive& value, NLValueStore& values) { _value = values.ownElement(value); }
    void reown(NLValueStore& values) {
        if (isKept()) {
            keep(*_value, values);
        }
    }

private:
    std::optional<Primitive> _value;
};

// A type-erased cell is kept inline, as a list holds it: its tag, then its value's bytes. A
// number, a boolean, an ID, a datetime or a duration is kept without a copy into the store; a
// string, list, map, embedding or path keeps the store's copy of what its bytes view.
template <>
class NLKeptValue<CellView> {
public:
    bool isKept() const { return _bytes[0] != NOTHING_KEPT; }

    // The kept cell, viewed where this state holds it
    CellView getValue() const { return CellView {_bytes}; }

    void keep(CellView cell, NLValueStore& values) {
        const CellTag tag = cell.getTag();

        CellTagDispatcher {tag}.execute([&]<typename T>(CellView view) {
            static_assert(sizeof(T) <= VALUE_BYTES, "A kept cell holds the value of every tag inline");

            T value = view.getAs<T>();
            if constexpr (NLViewElement<T>) {
                value = values.own(value);
            }

            _bytes[0] = std::byte {static_cast<uint8_t>(tag)};
            std::memcpy(_bytes + 1, &value, sizeof(T));
        }, cell);
    }

    void reown(NLValueStore& values) {
        if (isKept()) {
            keep(getValue(), values);
        }
    }

private:
    static constexpr size_t VALUE_BYTES = sizeof(types::Point::Primitive);
    static constexpr std::byte NOTHING_KEPT {static_cast<uint8_t>(CellTag::INVALID)};

    std::byte _bytes[1 + VALUE_BYTES] {NOTHING_KEPT};
};

// The state of an average: the total of the values folded and how many there were. A group
// that folded none averages to null.
template <typename Total>
struct NLAverageState {
    Total _total {};
    uint64_t _count {0};
};

}
