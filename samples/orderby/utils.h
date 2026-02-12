#pragma once

#include <iostream>
#include <memory>
#include <chrono>
#include <random>

#include <spdlog/spdlog.h>

#include "FatalException.h"
#include "sort.h"

#include "LocalMemory.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "metadata/PropertyType.h"

namespace db {

using Df = std::unique_ptr<Dataframe>;
using Int = types::Int64::Primitive;
using ColumnInts = ColumnVector<Int>;

// moves each vector into a ColumnVector<T> and adds that as a column
template <typename T>
Df makeDataframe(LocalMemory& mem, DataframeManager& dfman, std::vector<std::vector<T>>&& cols) {
    auto df = std::make_unique<Dataframe>();
    
    for (auto&& colData : cols) {
        auto* columnVec = mem.alloc<ColumnVector<T>>(std::move(colData));
        const ColumnTag tag = dfman.allocTag();
        NamedColumn* ncol = NamedColumn::create(&dfman, columnVec, tag);
        df->addColumn(ncol);
    }
    
    return df;
}

void duplicateDataframeShape(LocalMemory* mem,
                             DataframeManager* dfMan,
                             Dataframe* src,
                             Dataframe* dest) {
    for (const NamedColumn* col : src->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());
        dest->addColumn(newNamedCol);
    }
}

Df copyDataframe(LocalMemory& mem, DataframeManager& dfman, const Df& toCopy) {
    auto newDf = std::make_unique<Dataframe>();
    duplicateDataframeShape(&mem, &dfman, toCopy.get(), newDf.get());
    newDf->copyFrom(toCopy.get());
    return newDf;
}

// check each column in the df to see if its sorted
bool isSorted(const Df& df) {
    const size_t numRows = df->getRowCount();
    if (numRows <= 1) return true;
    
    const auto& cols = df->cols();
    
    // Compare each adjacent pair of rows lexicographically
    for (size_t row = 0; row < numRows - 1; row++) {
        // Compare row with row+1 across all columns
        for (NamedColumn* ncol : cols) {
            auto* ccol = ncol->as<ColumnInts>();
            Int val1 = (*ccol)[row];
            Int val2 = (*ccol)[row + 1];
            
            if (val1 < val2) {
                // row < row+1, this is correct ordering, check next pair
                break;
            } else if (val1 > val2) {
                // row > row+1, this violates sort order
                return false;
            }
            // val1 == val2, continue to next column for tiebreaker
        }
    }
    
    return true;
}

bool containSame(const Df& a, const Df& b) {
    if (a->getRowCount() != b->getRowCount()) {
        return false;
    }
    if (a->cols().size() != b->cols().size()) {
        return false;
    }

    auto makeRows = [](const Df& df) {
        const auto& cols = df->cols();
        size_t rows = df->getRowCount();

        std::vector<std::vector<Int>> out(rows);

        for (auto* ncol : cols) {
            auto* c = ncol->as<ColumnInts>();
            bioassert(c, "Non-int column");

            for (size_t r = 0; r < rows; ++r) {
                out[r].push_back((*c)[r]);
            }
        }

        std::ranges::sort(out);
        return out;
    };

    return makeRows(a) == makeRows(b);
}

Df makeRandomDataframe(LocalMemory& mem, DataframeManager& dfman, 
                       size_t numRows, size_t numCols,
                       Int minVal = 0, Int maxVal = 100) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<Int> dist(minVal, maxVal);
    
    std::vector<std::vector<Int>> cols;
    cols.reserve(numCols);
    
    for (size_t col = 0; col < numCols; col++) {
        std::vector<Int> colData;
        colData.reserve(numRows);
        
        for (size_t row = 0; row < numRows; row++) {
            colData.push_back(dist(gen));
        }
        
        cols.push_back(std::move(colData));
    }
    
    return makeDataframe<Int>(mem, dfman, std::move(cols));
}

// Overload with seed for reproducible tests
Df makeRandomDataframe(LocalMemory& mem, DataframeManager& dfman, 
                       size_t numRows, size_t numCols,
                       unsigned int seed,
                       Int minVal = 0, Int maxVal = 100) {
    std::mt19937 gen(seed);
    std::uniform_int_distribution<Int> dist(minVal, maxVal);
    
    std::vector<std::vector<Int>> cols;
    cols.reserve(numCols);
    
    for (size_t col = 0; col < numCols; col++) {
        std::vector<Int> colData;
        colData.reserve(numRows);
        
        for (size_t row = 0; row < numRows; row++) {
            colData.push_back(dist(gen));
        }
        
        cols.push_back(std::move(colData));
    }
    
    return makeDataframe<Int>(mem, dfman, std::move(cols));
}

void benchmarkSort(LocalMemory& mem, DataframeManager& dfman, size_t numRows,
                   size_t numCols, size_t iterations) {
    constexpr bool quiet = true;
    // Create the original random dataframe once
    auto original = makeRandomDataframe(mem, dfman, numRows, numCols);

    spdlog::info("Benchmarking sort on {}x{} dataframe over {} iterations", numRows,
                 numCols, iterations);

    std::vector<double> timings;
    timings.reserve(iterations);

    for (size_t i = 0; i < iterations; i++) {
        // Create a fresh copy for this iteration
        auto sorted = copyDataframe(mem, dfman, original);

        // Time the sort operation
        auto start = std::chrono::high_resolution_clock::now();
        colsort(sorted.get());
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

void compareSorts(LocalMemory& mem, DataframeManager& dfman,
                   size_t numRows, size_t numCols, size_t iterations) {
    // Create the original random dataframe once
    auto original = makeRandomDataframe(mem, dfman, numRows, numCols);
    
    spdlog::info("Benchmarking sort on {}x{} dataframe over {} iterations", 
                 numRows, numCols, iterations);
    
    std::vector<double> colsortTimings;
    std::vector<double> rowsortTimings;
    colsortTimings.reserve(iterations);
    rowsortTimings.reserve(iterations);
    
    for (size_t i = 0; i < iterations; i++) {
        // Benchmark colsort
        {
            auto sorted = copyDataframe(mem, dfman, original);
            
            auto start = std::chrono::high_resolution_clock::now();
            colsort(sorted.get());
            auto end = std::chrono::high_resolution_clock::now();
            
            std::chrono::duration<double, std::milli> duration = end - start;
            colsortTimings.push_back(duration.count());
            
            // Verify correctness
            if (!containSame(original, sorted)) {
                original->dump(std::cout);
                sorted->dump(std::cout);
                throw FatalException("colsort: Not same.");
            }
            if (!isSorted(sorted)) {
                original->dump(std::cout);
                sorted->dump(std::cout);
                throw FatalException("colsort: Not sorted.");
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
    
    // Calculate statistics for colsort
    double colsortAvg = std::accumulate(colsortTimings.begin(), colsortTimings.end(), 0.0) / iterations;
    double colsortMin = *std::min_element(colsortTimings.begin(), colsortTimings.end());
    double colsortMax = *std::max_element(colsortTimings.begin(), colsortTimings.end());
    
    // Calculate statistics for rowsort
    double rowsortAvg = std::accumulate(rowsortTimings.begin(), rowsortTimings.end(), 0.0) / iterations;
    double rowsortMin = *std::min_element(rowsortTimings.begin(), rowsortTimings.end());
    double rowsortMax = *std::max_element(rowsortTimings.begin(), rowsortTimings.end());
    
    // Calculate speedup
    double speedup = rowsortAvg / colsortAvg;
    
    fmt::print("\n=== BENCHMARK RESULTS ===\n");
    fmt::print("Dimensions: {} rows × {} columns\n", numRows, numCols);
    fmt::print("Iterations: {}\n\n", iterations);
    
    fmt::print("colsort:\n");
    fmt::print("  Average: {:.3f} ms\n", colsortAvg);
    fmt::print("  Min:     {:.3f} ms\n", colsortMin);
    fmt::print("  Max:     {:.3f} ms\n\n", colsortMax);
    
    fmt::print("ROWSORT:\n");
    fmt::print("  Average: {:.3f} ms\n", rowsortAvg);
    fmt::print("  Min:     {:.3f} ms\n", rowsortMin);
    fmt::print("  Max:     {:.3f} ms\n\n", rowsortMax);
    
    fmt::print("SPEEDUP: {:.2f}x {}\n", 
               std::abs(speedup),
               speedup > 1.0 ? "(colsort faster)" : "(rowsort faster)");
}

}
