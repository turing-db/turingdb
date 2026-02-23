#pragma once

#include "iterators/ChunkConfig.h"
#include "merge.h"
#include "utils.h"

namespace db {

inline void mergeTest(LocalMemory& mem, DataframeManager& dfman, size_t N, size_t numRuns, size_t numCols) {
    std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<Int> dist{std::numeric_limits<Int>::min(),
                                            std::numeric_limits<Int>::max()};

    // Generate one random vector per column
    std::vector<std::vector<Int>> colData(numCols, std::vector<Int>(N));
    for (auto& col : colData) {
        std::ranges::generate(col, [&] { return dist(rng); });
    }

    // Sort each run across all columns lexicographically
    std::vector<SortedRun> runs;
    size_t start = 0;
    for (size_t i = 0; i < numRuns && start < N; ++i) {
        size_t remaining = N - start;
        size_t runsLeft  = numRuns - i;
        size_t runSize   = remaining / runsLeft;
        if (runSize == 0) break;

        // Sort indices for this run lexicographically across all columns
        std::vector<size_t> runIdx(runSize);
        std::iota(runIdx.begin(), runIdx.end(), start);
        std::sort(runIdx.begin(), runIdx.end(), [&](size_t a, size_t b) {
            for (const auto& col : colData) {
                if (col[a] != col[b]) return col[a] < col[b];
            }
            return false;
        });

        // Apply the sorted permutation back into each column
        for (auto& col : colData) {
            std::vector<Int> tmp(runSize);
            for (size_t k = 0; k < runSize; ++k) tmp[k] = col[runIdx[k]];
            std::copy(tmp.begin(), tmp.end(), col.begin() + start);
        }

        runs.push_back({._start = start, ._size = runSize});
        start += runSize;
    }

    // Rebuild the dataframe with the per-run sorted data
    auto original = makeDataframe<Int>(mem, dfman, std::move(colData));
    auto sorted = copyDataframe(mem, dfman, original);

    std::vector<size_t> indices(N);
    std::ranges::iota(indices, 0);

    merge(indices, original->cols(), runs);

    const auto cols = sorted->cols()
                      | rv::transform([](const NamedColumn* ncol) {
                          return ncol->getColumn();
                      });

    for (Column* col : cols) {
        project(col, indices);
    }

    if (isSorted(sorted)) {
        spdlog::info("Merge test PASSED.");
    } else {
        spdlog::info("Merge test FAILED.");
        sorted->dump(std::cout);
    }
}

inline void benchmarkSort(LocalMemory& mem, DataframeManager& dfman, size_t numRows,
                   size_t numCols, size_t iterations, Int minV = 0, Int maxV = 1000) {
    constexpr bool quiet = true;
    // Create the original random dataframe once
    auto original = makeRandomDataframe(mem, dfman, numRows, numCols, minV, maxV);

    spdlog::info("Benchmarking sort on {}x{} dataframe over {} iterations", numRows,
                 numCols, iterations);

    std::vector<double> timings;
    timings.reserve(iterations);

    for (size_t i = 0; i < iterations; i++) {
        // Create a fresh copy for this iteration
        auto sorted = copyDataframe(mem, dfman, original);

        // Time the sort operation
        auto start = std::chrono::high_resolution_clock::now();
        subsort(sorted.get());
        auto end = std::chrono::high_resolution_clock::now();

        // Calculate duration in milliseconds
        std::chrono::duration<double, std::milli> duration = end - start;
        timings.push_back(duration.count());

        // Verify correctness
        if (!containSame(original, sorted)) {
            original->dump(std::cout);
            sorted->dump(std::cout);
            rowsort(original.get());
            original->dump(std::cout);
            throw FatalException("Not same.");
        }
        if (!isSorted(sorted)) {
            original->dump(std::cout);
            sorted->dump(std::cout);
            throw FatalException("Not sorted.");
        }
    }

    // Calculate statistics
    double total = std::accumulate(timings.begin(), timings.end(), 0.0);
    double average = total / iterations;
    double minTime = *std::min_element(timings.begin(), timings.end());
    double maxTime = *std::max_element(timings.begin(), timings.end());

    // Calculate standard deviation
    double variance = 0.0;
    for (double time : timings) {
        variance += (time - average) * (time - average);
    }
    double stddev = std::sqrt(variance / iterations);

    if (!quiet) {
        fmt::print("\n=== BENCHMARK RESULTS ===\n");
        fmt::print("Dimensions: {} rows × {} columns\n", numRows, numCols);
        fmt::print("Iterations: {}\n", iterations);
        fmt::print("Average time: {:.3f} ms\n", average);
        fmt::print("Min time: {:.3f} ms\n", minTime);
        fmt::print("Max time: {:.3f} ms\n", maxTime);
        fmt::print("Std deviation: {:.3f} ms\n", stddev);
        fmt::print("Total time: {:.3f} ms\n", total);
    }
}

inline void compareSorts(LocalMemory& mem, DataframeManager& dfman, size_t numRows,
                  size_t numCols, size_t iterations, Int minV = 0, Int maxV = 100) {
    // Create the original random dataframe once
    auto original = makeRandomDataframe(mem, dfman, numRows, numCols, minV, maxV);
    
    spdlog::info("Benchmarking sort on {}x{} dataframe over {} iterations", 
                 numRows, numCols, iterations);
    
    std::vector<double> subsortTimings;
    std::vector<double> rowsortTimings;
    subsortTimings.reserve(iterations);
    rowsortTimings.reserve(iterations);
    
    for (size_t i = 0; i < iterations; i++) {
        // Benchmark subsort
        {
            auto sorted = copyDataframe(mem, dfman, original);
            
            auto start = std::chrono::high_resolution_clock::now();
            subsort(sorted.get());
            auto end = std::chrono::high_resolution_clock::now();
            
            std::chrono::duration<double, std::milli> duration = end - start;
            subsortTimings.push_back(duration.count());
            
            // Verify correctness
            if (!containSame(original, sorted)) {
                original->dump(std::cout);
                sorted->dump(std::cout);
                throw FatalException("subsort: Not same.");
            }
            if (!isSorted(sorted)) {
                original->dump(std::cout);
                sorted->dump(std::cout);
                throw FatalException("subsort: Not sorted.");
            }
        }
        
        // Benchmark rowsort
        {
            auto sorted = copyDataframe(mem, dfman, original);
            
            auto start = std::chrono::high_resolution_clock::now();
            rowsort(sorted.get());
            auto end = std::chrono::high_resolution_clock::now();
            
            std::chrono::duration<double, std::milli> duration = end - start;
            rowsortTimings.push_back(duration.count());
            
            // Verify correctness
            if (!containSame(original, sorted)) {
                original->dump(std::cout);
                sorted->dump(std::cout);
                throw FatalException("rowsort: Not same.");
            }
            if (!isSorted(sorted)) {
                original->dump(std::cout);
                sorted->dump(std::cout);
                throw FatalException("rowsort: Not sorted.");
            }
        }
    }
    
    // Calculate statistics for subsort
    double subsortAvg = std::accumulate(subsortTimings.begin(), subsortTimings.end(), 0.0) / iterations;
    double subsortMin = *std::min_element(subsortTimings.begin(), subsortTimings.end());
    double subsortMax = *std::max_element(subsortTimings.begin(), subsortTimings.end());
    
    // Calculate statistics for rowsort
    double rowsortAvg = std::accumulate(rowsortTimings.begin(), rowsortTimings.end(), 0.0) / iterations;
    double rowsortMin = *std::min_element(rowsortTimings.begin(), rowsortTimings.end());
    double rowsortMax = *std::max_element(rowsortTimings.begin(), rowsortTimings.end());
    
    // Calculate speedup
    double speedup = rowsortAvg / subsortAvg;
    
    fmt::print("\n=== BENCHMARK RESULTS ===\n");
    fmt::print("Dimensions: {} rows × {} columns\n", numRows, numCols);
    fmt::print("Iterations: {}\n\n", iterations);
    
    fmt::print("subsort:\n");
    fmt::print("  Average: {:.3f} ms\n", subsortAvg);
    fmt::print("  Min:     {:.3f} ms\n", subsortMin);
    fmt::print("  Max:     {:.3f} ms\n\n", subsortMax);
    
    fmt::print("ROWSORT:\n");
    fmt::print("  Average: {:.3f} ms\n", rowsortAvg);
    fmt::print("  Min:     {:.3f} ms\n", rowsortMin);
    fmt::print("  Max:     {:.3f} ms\n\n", rowsortMax);
    
    fmt::print("SPEEDUP: {:.2f}x {}\n", 
               std::abs(speedup),
               speedup > 1.0 ? "(subsort faster)" : "(rowsort faster)");
}

inline void benchmarkMerge(LocalMemory& mem, DataframeManager& dfman) {
    constexpr size_t CS = ChunkConfig::CHUNK_SIZE;
    struct Case { size_t N, runs, cols; };
    constexpr std::array cases = {
        Case {.N = CS * 3, .runs = 3, .cols = 5},
        Case {.N = CS * 10, .runs = 15, .cols = 3},
        Case {.N = CS * 20, .runs = 20, .cols = 1},
        Case {.N = CS * 20, .runs = 20, .cols = 2},
        Case {.N = CS * 20, .runs = 20, .cols = 3},
        Case {.N = CS * 20, .runs = 20, .cols = 5},
    };

    for (auto [N, runs, cols] : cases) {
        spdlog::info("--- N={}, runs={}, cols={} ---", N, runs, cols);
        auto start = std::chrono::high_resolution_clock::now();
        mergeTest(mem, dfman, N, runs, cols);
        auto end = std::chrono::high_resolution_clock::now();
        auto us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        spdlog::info("Completed in {}us ({}ms)", us, us / 1000);
    }
}

}
