#include "HdrHistogram.h"

#include <math.h>

#include <algorithm>
#include <bit>
#include <ostream>

#include <spdlog/fmt/fmt.h>

#include "BioAssert.h"
#include "TuringException.h"

using namespace forge;

HdrHistogram::HdrHistogram(uint64_t lowestDiscernibleValue,
                           uint64_t highestTrackableValue,
                           size_t significantValueDigits)
    : _lowestDiscernibleValue(lowestDiscernibleValue),
    _highestTrackableValue(highestTrackableValue),
    _significantValueDigits(significantValueDigits) {
}

void HdrHistogram::init() {
    if (_lowestDiscernibleValue < 1) {
        throw TuringException("HdrHistogram: lowestDiscernibleValue must be >= 1");
    } else if (_significantValueDigits < 1 || _significantValueDigits > 5) {
        throw TuringException("HdrHistogram: significantValueDigits must be in [1, 5]");
    } else if (_highestTrackableValue < 2 * _lowestDiscernibleValue) {
        throw TuringException("HdrHistogram: highestTrackableValue must be >= 2 * lowestDiscernibleValue");
    }

    _unitMagnitude = static_cast<int>(floor(log2(static_cast<double>(_lowestDiscernibleValue))));

    // The number of distinct values we want to resolve at single-unit precision
    // before relative bucketing takes over.
    const uint64_t largestValueWithSingleUnitResolution =
        2 * static_cast<uint64_t>(pow(10.0, static_cast<double>(_significantValueDigits)));

    const int subBucketCountMagnitude =
        static_cast<int>(ceil(log2(static_cast<double>(largestValueWithSingleUnitResolution))));

    _subBucketHalfCountMagnitude = (subBucketCountMagnitude > 1 ? subBucketCountMagnitude : 1) - 1;
    _subBucketCount = static_cast<int>(pow(2.0, static_cast<double>(_subBucketHalfCountMagnitude + 1)));
    _subBucketHalfCount = _subBucketCount / 2;
    _subBucketMask = (static_cast<uint64_t>(_subBucketCount) - 1) << _unitMagnitude;

    _leadingZeroCountBase = 64 - _unitMagnitude - (_subBucketHalfCountMagnitude + 1);

    // Count how many power-of-two buckets are needed to reach the highest
    // trackable value, starting from the first value the sub-buckets cannot
    // cover on their own.
    uint64_t smallestUntrackableValue = static_cast<uint64_t>(_subBucketCount) << _unitMagnitude;
    int bucketsNeeded = 1;
    while (smallestUntrackableValue <= _highestTrackableValue) {
        if (smallestUntrackableValue > (UINT64_MAX / 2)) {
            ++bucketsNeeded;
            break;
        }

        smallestUntrackableValue <<= 1;
        ++bucketsNeeded;
    }
    _bucketCount = bucketsNeeded;

    const size_t countsArrayLength =
        static_cast<size_t>(_bucketCount + 1) * static_cast<size_t>(_subBucketHalfCount);
    _counts.assign(countsArrayLength, 0);
}

HdrHistogram::~HdrHistogram() = default;

int HdrHistogram::bucketIndexFor(uint64_t value) const {
    return _leadingZeroCountBase - std::countl_zero(value | _subBucketMask);
}

int HdrHistogram::subBucketIndexFor(uint64_t value, int bucketIndex) const {
    return static_cast<int>(value >> (bucketIndex + _unitMagnitude));
}

size_t HdrHistogram::countsIndex(int bucketIndex, int subBucketIndex) const {
    const int bucketBaseIndex = (bucketIndex + 1) << _subBucketHalfCountMagnitude;
    const int offsetInBucket = subBucketIndex - _subBucketHalfCount;
    return static_cast<size_t>(bucketBaseIndex + offsetInBucket);
}

size_t HdrHistogram::countsIndexFor(uint64_t value) const {
    const int bucketIndex = bucketIndexFor(value);
    const int subBucketIndex = subBucketIndexFor(value, bucketIndex);
    return countsIndex(bucketIndex, subBucketIndex);
}

uint64_t HdrHistogram::valueFromBucket(int bucketIndex, int subBucketIndex) const {
    return static_cast<uint64_t>(subBucketIndex) << (bucketIndex + _unitMagnitude);
}

uint64_t HdrHistogram::valueFromIndex(size_t index) const {
    int bucketIndex = static_cast<int>(index >> _subBucketHalfCountMagnitude) - 1;
    int subBucketIndex =
        static_cast<int>(index & static_cast<size_t>(_subBucketHalfCount - 1)) + _subBucketHalfCount;

    if (bucketIndex < 0) {
        subBucketIndex -= _subBucketHalfCount;
        bucketIndex = 0;
    }

    return valueFromBucket(bucketIndex, subBucketIndex);
}

void HdrHistogram::record(uint64_t value) {
    recordValues(value, 1);
}

void HdrHistogram::recordValues(uint64_t value, uint64_t count) {
    uint64_t recordedValue = value;
    bool outOfRange = false;
    if (recordedValue > _highestTrackableValue) {
        recordedValue = _highestTrackableValue;
        outOfRange = true;
    }

    const size_t index = countsIndexFor(recordedValue);
    bioassert(index < _counts.size(),
              "HdrHistogram counts index {} out of bounds (size {})",
              index,
              _counts.size());

    const bool wasEmpty = (_totalCount == 0);

    _counts[index] += count;
    _totalCount += count;
    if (outOfRange) {
        _outOfRangeCount += count;
    }

    if (wasEmpty || recordedValue < _minValue) {
        _minValue = recordedValue;
    }
    if (wasEmpty || recordedValue > _maxValue) {
        _maxValue = recordedValue;
    }
}

void HdrHistogram::add(const HdrHistogram& other) {
    const bool sameConfig =
        _lowestDiscernibleValue == other._lowestDiscernibleValue &&
        _highestTrackableValue == other._highestTrackableValue &&
        _significantValueDigits == other._significantValueDigits &&
        _counts.size() == other._counts.size();
    if (!sameConfig) {
        throw TuringException("HdrHistogram::add requires an identical histogram configuration");
    }

    if (other._totalCount == 0) {
        return;
    }

    for (size_t index = 0; index < _counts.size(); ++index) {
        _counts[index] += other._counts[index];
    }

    const bool wasEmpty = (_totalCount == 0);
    _totalCount += other._totalCount;
    _outOfRangeCount += other._outOfRangeCount;

    if (wasEmpty || other._minValue < _minValue) {
        _minValue = other._minValue;
    }
    if (wasEmpty || other._maxValue > _maxValue) {
        _maxValue = other._maxValue;
    }
}

void HdrHistogram::reset() {
    std::fill(_counts.begin(), _counts.end(), 0);
    _totalCount = 0;
    _outOfRangeCount = 0;
    _minValue = 0;
    _maxValue = 0;
}

uint64_t HdrHistogram::getMinValue() const {
    if (_totalCount == 0) {
        return 0;
    }
    return _minValue;
}

uint64_t HdrHistogram::getMaxValue() const {
    if (_totalCount == 0) {
        return 0;
    }
    return _maxValue;
}

uint64_t HdrHistogram::getValueAtPercentile(double percentile) const {
    if (_totalCount == 0) {
        return 0;
    }

    const double clampedPercentile = std::min(std::max(percentile, 0.0), 100.0);
    const double countAtPercentileDouble = (clampedPercentile / 100.0) * static_cast<double>(_totalCount);

    uint64_t countAtPercentile = static_cast<uint64_t>(countAtPercentileDouble + 0.5);
    if (countAtPercentile < 1) {
        countAtPercentile = 1;
    }

    uint64_t totalToCurrentIndex = 0;
    for (size_t index = 0; index < _counts.size(); ++index) {
        totalToCurrentIndex += _counts[index];
        if (totalToCurrentIndex >= countAtPercentile) {
            const uint64_t valueAtIndex = valueFromIndex(index);
            if (clampedPercentile <= 0.0) {
                return getLowestEquivalentValue(valueAtIndex);
            }
            return getHighestEquivalentValue(valueAtIndex);
        }
    }

    return 0;
}

double HdrHistogram::getMean() const {
    if (_totalCount == 0) {
        return 0.0;
    }

    double weightedTotal = 0.0;
    for (size_t index = 0; index < _counts.size(); ++index) {
        const uint64_t count = _counts[index];
        if (count != 0) {
            const double value = static_cast<double>(medianEquivalentValue(valueFromIndex(index)));
            weightedTotal += static_cast<double>(count) * value;
        }
    }

    return weightedTotal / static_cast<double>(_totalCount);
}

double HdrHistogram::getStdDeviation() const {
    if (_totalCount == 0) {
        return 0.0;
    }

    const double mean = getMean();
    double squaredDeviationTotal = 0.0;
    for (size_t index = 0; index < _counts.size(); ++index) {
        const uint64_t count = _counts[index];
        if (count != 0) {
            const double deviation = static_cast<double>(medianEquivalentValue(valueFromIndex(index))) - mean;
            squaredDeviationTotal += (deviation * deviation) * static_cast<double>(count);
        }
    }

    return sqrt(squaredDeviationTotal / static_cast<double>(_totalCount));
}

uint64_t HdrHistogram::getSizeOfEquivalentValueRange(uint64_t value) const {
    const int bucketIndex = bucketIndexFor(value);
    return static_cast<uint64_t>(1) << (_unitMagnitude + bucketIndex);
}

uint64_t HdrHistogram::getLowestEquivalentValue(uint64_t value) const {
    const int bucketIndex = bucketIndexFor(value);
    const int subBucketIndex = subBucketIndexFor(value, bucketIndex);
    return valueFromBucket(bucketIndex, subBucketIndex);
}

uint64_t HdrHistogram::getHighestEquivalentValue(uint64_t value) const {
    return getLowestEquivalentValue(value) + getSizeOfEquivalentValueRange(value) - 1;
}

uint64_t HdrHistogram::medianEquivalentValue(uint64_t value) const {
    return getLowestEquivalentValue(value) + (getSizeOfEquivalentValueRange(value) >> 1);
}

bool HdrHistogram::valuesAreEquivalent(uint64_t first, uint64_t second) const {
    return getLowestEquivalentValue(first) == getLowestEquivalentValue(second);
}

void HdrHistogram::printStats(std::ostream& output, const std::string& label, const std::string& unit) const {
    output << fmt::format("{} ({})  samples={}  out-of-range={}\n",
                          label,
                          unit,
                          _totalCount,
                          _outOfRangeCount);

    if (_totalCount == 0) {
        output << "  (no samples)\n";
        return;
    }

    output << fmt::format("  min  {:>12}\n", getMinValue());
    output << fmt::format("  avg  {:>12.2f}\n", getMean());
    output << fmt::format("  p50  {:>12}\n", getValueAtPercentile(50.0));
    output << fmt::format("  p90  {:>12}\n", getValueAtPercentile(90.0));
    output << fmt::format("  p99  {:>12}\n", getValueAtPercentile(99.0));
    output << fmt::format("  max  {:>12}\n", getMaxValue());
}
