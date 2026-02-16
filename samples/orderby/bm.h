#pragma once

#include "utils.h"

namespace db {

void benchmarkSort(LocalMemory& mem, DataframeManager& dfman, size_t numRows,
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

void compareSorts(LocalMemory& mem, DataframeManager& dfman, size_t numRows,
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

}
