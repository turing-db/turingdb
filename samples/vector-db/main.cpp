#include <spdlog/fmt/fmt.h>

#include "VecLibAccessor.h"
#include "VectorSearchQuery.h"
#include "BatchVectorCreate.h"
#include "VecLib.h"
#include "VectorDatabase.h"
#include "VectorSearchResult.h"
#include "RandomGenerator.h"
#include "TuringTime.h"

int main(int argc, char** argv) {
    const fs::Path rootPath {SAMPLE_DIR "/storage"};
    vec::RandomGenerator::initialize(1);

    std::string_view libName = "test2";

    // Creating a database
    auto dbRes = vec::VectorDatabase::create(rootPath);

    if (!dbRes) {
        fmt::println("Could not create database. {}", dbRes.error().fmtMessage());
        return 1;
    }

    auto db = std::move(dbRes.value());

    fmt::println("Initialized database at {}", rootPath.c_str());

    constexpr vec::Dimension dim = 1024;
    constexpr vec::DistanceMetric metric = vec::DistanceMetric::INNER_PRODUCT;
    constexpr size_t vecCount = 10'000;
    constexpr size_t batchCount = 5;

    // Vector to search
    std::vector<float> queryData(dim);
    for (size_t i = 0; i < dim; i++) {
        queryData[i] = vec::RandomGenerator::generate<float>();
    }

    if (!db->libraryExists(libName)) {
        // Creating a library
        const vec::VectorResult<vec::VecLibID> createRes = db->createLibrary(libName, dim, metric);

        if (!createRes) {
            fmt::println("Could not create library. {}", createRes.error().fmtMessage());
            return 1;
        }

        const vec::VecLibID libID = createRes.value();

        vec::VecLibAccessor lib = db->getLibrary(libID);
        if (!lib.isValid()) {
            fmt::println("Library not found");
            return 1;
        }

        fmt::println("- Created library with ID {}", libID);
        fmt::println("- Will allocate {} vectors", vecCount);

        constexpr size_t vecCountPerBatch = vecCount / batchCount;

        auto t0 = Clock::now();
        auto t1 = Clock::now();

        float dur = 0.0f;

        std::vector<float> vec(dim);

        // Generating vectors
        {
            vec::BatchVectorCreate batch = lib.prepareCreateBatch();

            for (size_t b = 0; b < batchCount; b++) {
                batch.clear(dim);
                fmt::println("    * Generating {} vectors", vecCountPerBatch);

                t1 = Clock::now();
                for (size_t i = 0; i < vecCountPerBatch; i++) {
                    for (size_t j = 0; j < dim; j++) {
                        vec[j] = vec::RandomGenerator::generate<float>();
                    }
                    batch.addPoint(i, vec);
                }
                dur = duration<Milliseconds>(t1, Clock::now());
                fmt::println("      Generated {} vectors in {:.2f} ms", vecCountPerBatch, dur);

                t1 = Clock::now();
                if (auto res = lib.addEmbeddings(batch); !res) {
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

    vec::VecLibAccessor lib = db->getLibrary(libName);

    if (!lib.isValid()) {
        fmt::println("Library not found");
        return 1;
    }

    vec::VectorSearchQuery query {dim};
    query.setMaxResultCount(20);
    query.setVector(queryData);

    vec::VectorSearchResult results;

    auto t0Search = Clock::now();

    if (auto res = lib.search(query, results); !res) {
        fmt::println("Could not search vectors. {}", res.error().fmtMessage());
        return 1;
    }

    fmt::println("Found {} results in {} ms", results.count(), duration<Milliseconds>(t0Search, Clock::now()));

    const std::span distances = results.distances();
    const std::span ids = results.ids();
    const std::span shards = results.shards();

    if (results.count() == 0) {
        fmt::println("No results found");
        return 0;
    }

    for (size_t i = 0; i < results.count(); i++) {
        fmt::println(" * Result [{}]: {}. Shard={} Distance={:.4f}", i, ids[i], shards[i], distances[i]);
    }

    return 0;
}
