#include <iostream>
#include <spdlog/fmt/fmt.h>
#include <random>

#include "BatchVectorSearch.h"
#include "BatchVectorCreate.h"
#include "VecLib.h"
#include "VecLibShard.h"
#include "VectorDatabase.h"
#include "VectorSearchResult.h"
#include "TuringTime.h"

int main(int argc, char** argv) {
    const fs::Path rootPath {SAMPLE_DIR "/storage"};

    // Random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> rand(0.0f, 1.0f);

    // Creating a database
    auto dbRes = vec::VectorDatabase::create(rootPath);

    if (!dbRes) {
        fmt::println("Could not create database: {}", dbRes.error().fmtMessage());
        return 1;
    }

    auto db = std::move(dbRes.value());

    fmt::println("Initialized database at {}", rootPath.c_str());

    constexpr vec::Dimension dim = 512;
    constexpr vec::DistanceMetric metric = vec::DistanceMetric::INNER_PRODUCT;
    constexpr size_t vecCount = 1'000'000;
    constexpr size_t memUsage = vecCount * dim * sizeof(float);
    constexpr size_t batchCount = 10;

    // Creating a library
    const vec::VectorResult<vec::VecLibID> createRes = db->createLibrary("test", dim, metric);

    if (!createRes) {
        fmt::println("Could not create library: {}", createRes.error().fmtMessage());
        return 1;
    }

    const vec::VecLibID libID = createRes.value();

    vec::VecLib* lib = db->getLibrary(libID);
    if (!lib) {
        fmt::println("Library not found");
        return 1;
    }

    fmt::println("- Created library with ID {}", libID);
    fmt::println("- Will allocate {} vectors ({} MiB)", vecCount, memUsage / (1024ul * 1024));

    constexpr size_t vecCountPerBatch = vecCount / batchCount;

    auto t0Gen = Clock::now();

    float genTime = 0.0f;

    // Generating vectors
    {
        vec::BatchVectorCreate batch {dim};
        auto& vec = batch.embeddings();
        auto& extIDs = batch.externalIDs();

        vec.resize(vecCountPerBatch * dim);
        extIDs.resize(vecCountPerBatch);

        for (size_t b = 0; b < batchCount; b++) {
            constexpr size_t memPerBatch = vecCountPerBatch * dim * sizeof(float);
            fmt::println("    * Generating {} vectors ({} MiB)", vecCountPerBatch, memPerBatch / (1024ul * 1024));

            t0Gen = Clock::now();
            for (size_t i = 0; i < vecCountPerBatch * dim; i++) {
                vec[i] = rand(gen);
            }

            // Generating external IDs
            std::iota(extIDs.begin(), extIDs.end(), 0);

            if (auto res = lib->addEmbeddings(batch); !res) {
                fmt::println("Could not add vectors: {}", res.error().fmtMessage());
                return 1;
            }
            genTime += duration<Milliseconds>(t0Gen, Clock::now());
        }
    }

    fmt::println("- Generated vectors in {:.2f} s", genTime / 1000.0f);
    fmt::println("- Vector library has {} shards", lib->getShardCount());

    for (size_t i = 0; i < lib->getShardCount(); i++) {
        const auto& shard = lib->getShard(i);
        fmt::println("Shard {}", i);
        const float memUsage = (float)shard.getUsedMem() / 1024.0f / 1024.0f;
        fmt::println("  - Shard {}: has {} vectors ({} MiB)", i, shard._vecCount, memUsage);
    }

    // Searching vectors
    std::vector<float> queryData(dim);
    for (size_t i = 0; i < dim; i++) {
        queryData[i] = rand(gen);
    }

    vec::BatchVectorSearch query {dim};
    query.setResultCount(1);
    query.addPoint(queryData);

    vec::VectorSearchResult results;

    auto t0Search = Clock::now();

    if (auto res = lib->search(query, results); !res) {
        fmt::println("Could not search vectors: {}", res.error().fmtMessage());
        return 1;
    }

    fmt::println("Found {} results in {} ms", results.count(), duration<Milliseconds>(t0Search, Clock::now()));

    const std::span distances = results.distances();
    const std::span indices = results.indices();
    const std::span shards = results.shards();

    for (size_t i = 0; i < results.count(); i++) {
        fmt::println(" * Result [{}]: {}. Shard={} Distance={:.4f}", i, indices[i], shards[i], distances[i]);
    }

    int res {};
    fmt::print("Press any key to exit...");
    std::cin >> res;

    return 0;
}
