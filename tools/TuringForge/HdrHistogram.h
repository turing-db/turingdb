#pragma once

#include <stdint.h>
#include <stddef.h>

#include <iosfwd>
#include <string>
#include <vector>

namespace forge {

// A High Dynamic Range (HDR) histogram: records integer values across a very
// wide range at a fixed relative precision, in bounded memory. Built for
// latency measurement, where samples span many orders of magnitude (e.g.
// nanoseconds to seconds) yet every percentile must stay accurate to a chosen
// number of significant digits.
//
// Values are bucketed logarithmically; each power-of-two bucket is split into a
// fixed number of linear sub-buckets, so the relative resolution is constant
// across the whole range. Recording is O(1) and allocation-free after
// construction, which makes it safe to call on a benchmark hot path.
class HdrHistogram {
public:
    // lowestDiscernibleValue   Smallest value that must be distinguishable from
    //                          zero (>= 1; typically 1).
    // highestTrackableValue    Largest value recordable without saturating
    //                          (>= 2 * lowestDiscernibleValue).
    // significantValueDigits   Precision in the range [1, 5]: the number of
    //                          significant decimal digits preserved for every
    //                          recorded value.
    HdrHistogram(uint64_t lowestDiscernibleValue,
                 uint64_t highestTrackableValue,
                 size_t significantValueDigits);
    ~HdrHistogram();

    HdrHistogram(const HdrHistogram&) = delete;
    HdrHistogram(HdrHistogram&&) = delete;
    HdrHistogram& operator=(const HdrHistogram&) = delete;
    HdrHistogram& operator=(HdrHistogram&&) = delete;

    // Validate the configuration, derive the bucket layout, and allocate the bucket
    // storage. Must be called once before recording. Throws TuringException if the
    // configuration passed to the constructor is invalid.
    void init();

    // Record a single occurrence of value. Values above highestTrackableValue
    // are clamped to the top bucket and also counted in getOutOfRangeCount().
    void record(uint64_t value);

    // Record count occurrences of value in one operation.
    void recordValues(uint64_t value, uint64_t count);

    // Merge every recorded value of other into this histogram. Both histograms
    // must share an identical configuration. Use this to fold per-thread
    // histograms into a single global one once a benchmark run has finished.
    void add(const HdrHistogram& other);

    // Drop all recorded values, keeping the configuration.
    void reset();

    uint64_t getTotalCount() const { return _totalCount; }
    uint64_t getOutOfRangeCount() const { return _outOfRangeCount; }
    bool isEmpty() const { return _totalCount == 0; }

    // Smallest / largest recorded value (0 when empty).
    uint64_t getMinValue() const;
    uint64_t getMaxValue() const;

    double getMean() const;
    double getStdDeviation() const;

    // Value below which `percentile` percent of recorded values fall.
    // percentile is clamped to [0, 100]; returns 0 when empty.
    uint64_t getValueAtPercentile(double percentile) const;

    // The lowest/highest value mapping to the same bucket as value, and the
    // width of that bucket. Two values are "equivalent" when the histogram
    // cannot tell them apart.
    uint64_t getLowestEquivalentValue(uint64_t value) const;
    uint64_t getHighestEquivalentValue(uint64_t value) const;
    uint64_t getSizeOfEquivalentValueRange(uint64_t value) const;
    bool valuesAreEquivalent(uint64_t first, uint64_t second) const;

    // Print a summary block (sample count, min, mean, p50/p90/p99, max) to
    // `output`. `label` names the metric on the header line (e.g. "latency",
    // "engine") and `unit` is its unit (e.g. "us", "ms"); recorded values are
    // printed verbatim in whatever unit they were recorded in.
    void printStats(std::ostream& output,
                    const std::string& label = "latency",
                    const std::string& unit = "us") const;

    uint64_t getLowestDiscernibleValue() const { return _lowestDiscernibleValue; }
    uint64_t getHighestTrackableValue() const { return _highestTrackableValue; }
    size_t getSignificantValueDigits() const { return _significantValueDigits; }

private:
    // Map a value to its slot in _counts.
    size_t countsIndexFor(uint64_t value) const;
    int bucketIndexFor(uint64_t value) const;
    int subBucketIndexFor(uint64_t value, int bucketIndex) const;
    size_t countsIndex(int bucketIndex, int subBucketIndex) const;

    // Reconstruct the representative value for a _counts slot.
    uint64_t valueFromIndex(size_t index) const;
    uint64_t valueFromBucket(int bucketIndex, int subBucketIndex) const;
    uint64_t medianEquivalentValue(uint64_t value) const;

    uint64_t _lowestDiscernibleValue {1};
    uint64_t _highestTrackableValue {1};
    size_t _significantValueDigits {3};

    // Derived layout parameters, all fixed at construction.
    int _unitMagnitude {0};
    int _subBucketHalfCountMagnitude {0};
    int _subBucketCount {0};
    int _subBucketHalfCount {0};
    uint64_t _subBucketMask {0};
    int _leadingZeroCountBase {0};
    int _bucketCount {0};

    std::vector<uint64_t> _counts;
    uint64_t _totalCount {0};
    uint64_t _outOfRangeCount {0};
    uint64_t _minValue {0};
    uint64_t _maxValue {0};
};

}
