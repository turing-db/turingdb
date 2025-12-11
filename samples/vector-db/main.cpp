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
    // std::random_device rd;
    std::mt19937 gen(1); // NOLINT
    std::uniform_real_distribution<float> rand(0.0f, 1.0f);
    std::string_view libName = "test2";

    // Creating a database
    auto dbRes = vec::VectorDatabase::create(rootPath);

    if (!dbRes) {
        fmt::println("Could not create database. {}", dbRes.error().fmtMessage());
        return 1;
    }

    auto db = std::move(dbRes.value());

    fmt::println("Initialized database at {}", rootPath.c_str());

    constexpr vec::Dimension dim = 256;
    constexpr vec::DistanceMetric metric = vec::DistanceMetric::INNER_PRODUCT;
    constexpr size_t vecCount = 1'000'000;
    constexpr size_t batchCount = 20;
    constexpr size_t memUsage = vecCount * dim * sizeof(float) // Vectors
                              + vecCount * sizeof(uint64_t);   // External IDs

    // Vector to search
    std::vector<float> queryData(dim);
    for (size_t i = 0; i < dim; i++) {
        queryData[i] = rand(gen);
    }

    if (!db->libraryExists(libName)) {
        // Creating a library
        const vec::VectorResult<vec::VecLibID> createRes = db->createLibrary(libName, dim, metric);

        if (!createRes) {
            fmt::println("Could not create library. {}", createRes.error().fmtMessage());
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

        auto t0 = Clock::now();
        auto t1 = Clock::now();

        float dur = 0.0f;

        std::vector<float> vec(dim);

        // Generating vectors
        {
            vec::BatchVectorCreate batch = lib->prepareCreateBatch(dim);

            for (size_t b = 0; b < batchCount; b++) {
                batch.clear();
                constexpr size_t memPerBatch = vecCountPerBatch * dim * sizeof(float);
                fmt::println("    * Generating {} vectors ({} MiB)", vecCountPerBatch, memPerBatch / (1024ul * 1024));

                t1 = Clock::now();
                for (size_t i = 0; i < vecCountPerBatch; i++) {
                    for (size_t j = 0; j < dim; j++) {
                        vec[j] = rand(gen);
                    }
                    batch.addPoint(i, vec);
                }
                dur = duration<Milliseconds>(t1, Clock::now());
                fmt::println("      Generated {} vectors in {:.2f} ms", vecCountPerBatch, dur);

                t1 = Clock::now();
                if (auto res = lib->addEmbeddings(batch); !res) {
                    fmt::println("Could not add vectors: {}", res.error().fmtMessage());
                    return 1;
                }
                dur = duration<Milliseconds>(t1, Clock::now());
                fmt::println("      Added {} vectors in {:.2f} ms", vecCountPerBatch, dur);
            }
        }

        float totalDur = duration<Seconds>(t0, Clock::now());
        fmt::println("- Generated vectors in {:.2f} s", totalDur);
    }

    vec::VecLib* lib = db->getLibrary(libName);

    vec::BatchVectorSearch query {dim};
    query.setMaxResultCount(20);
    query.setVector(queryData);

    vec::VectorSearchResult results;

    auto t0Search = Clock::now();

    if (auto res = lib->search(query, results); !res) {
        fmt::println("Could not search vectors. {}", res.error().fmtMessage());
        return 1;
    }

    fmt::println("Found {} results in {} ms", results.count(), duration<Milliseconds>(t0Search, Clock::now()));

    const std::span distances = results.distances();
    const std::span indices = results.indices();
    const std::span shards = results.shards();

    if (results.count() == 0) {
        fmt::println("No results found");
        return 0;
    }

    for (size_t i = 0; i < results.count(); i++) {
        fmt::println(" * Result [{}]: {}. Shard={} Distance={:.4f}", i, indices[i], shards[i], distances[i]);
    }

    return 0;
}
