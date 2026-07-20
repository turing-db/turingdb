#include "TuringTest.h"

#include <algorithm>
#include <random>
#include <span>
#include <unordered_set>
#include <vector>

#include "VecLibAccessor.h"
#include "VectorDatabase.h"
#include "BatchVectorCreate.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"

using namespace vec;
using namespace turing::test;

// Measures the recall of the approximate HNSW index against exact (brute-force)
// ground truth. HNSW trades a small amount of accuracy for speed, so recall is
// below 100% in general; the test builds an index over a moderate random dataset,
// compares HNSW's top-k against the true top-k for many queries, and requires the
// averaged recall to stay above 90%.
class VecLibHNSWRecallTest : public TuringTest {
public:
    void initialize() override {
        TuringTest::initialize();
        _rootDir = fs::Path {_outDir} / "veclib_hnsw_recall";

        if (!_rootDir.exists()) {
            ASSERT_TRUE(_rootDir.mkdir());
        }
    }

protected:
    static constexpr Dimension dimension = 32;
    static constexpr size_t vectorCount = 2000;
    static constexpr size_t queryCount = 100;
    static constexpr size_t topK = 10;
    static constexpr double recallThreshold = 0.90;

    // Fixed seeds keep the dataset and queries reproducible. They differ so the
    // queries are not a prefix of the dataset's random stream, which would make
    // every query an exact stored point and defeat the recall measurement.
    static constexpr uint32_t datasetSeed = 1234u;
    static constexpr uint32_t querySeed = 9876u;

    fs::Path _rootDir;

    // Fills a flat count-by-dimension row-major array with deterministic uniform
    // random values. A fixed seed keeps the dataset (and therefore the measured
    // recall) reproducible across runs.
    void generateVectors(uint32_t seed, size_t count, std::vector<float>& out) {
        std::mt19937 engine(seed);
        std::uniform_real_distribution<float> distribution(0.0f, 1.0f);

        out.resize(count * dimension);

        for (float& value : out) {
            value = distribution(engine);
        }
    }

    // Computes the exact top-k nearest-neighbour ids for a query under squared
    // euclidean distance by scanning every stored vector.
    void exactTopK(std::span<const float> data,
                   size_t count,
                   std::span<const float> query,
                   size_t k,
                   std::unordered_set<int64_t>& outIds) {
        std::vector<std::pair<float, int64_t>> scored(count);

        for (size_t i = 0; i < count; i++) {
            const float* vector = data.data() + i * dimension;

            float squaredDistance = 0.0f;
            for (size_t j = 0; j < dimension; j++) {
                const float delta = vector[j] - query[j];
                squaredDistance += delta * delta;
            }

            scored[i] = {squaredDistance, static_cast<int64_t>(i)};
        }

        const size_t resultCount = std::min(k, count);
        std::partial_sort(scored.begin(), scored.begin() + resultCount, scored.end());

        outIds.clear();
        for (size_t i = 0; i < resultCount; i++) {
            outIds.insert(scored[i].second);
        }
    }
};

TEST_F(VecLibHNSWRecallTest, recallAboveThreshold) {
    std::vector<float> dataset;
    generateVectors(datasetSeed, vectorCount, dataset);

    std::vector<float> queries;
    generateVectors(querySeed, queryCount, queries);

    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    ASSERT_TRUE(db.createLibrary("hnsw_recall", dimension, DistanceMetric::EUCLIDEAN_DIST, IndexType::HNSW));

    // Ingest the whole dataset in a single batch.
    {
        VecLibAccessor accessor = db.getLibrary("hnsw_recall");
        ASSERT_TRUE(accessor.isValid());

        BatchVectorCreate batch;
        accessor.prepareCreateBatch(&batch);

        for (size_t i = 0; i < vectorCount; i++) {
            const std::span<const float> point(dataset.data() + i * dimension, dimension);
            batch.addPoint(static_cast<int64_t>(i), point);
        }

        ASSERT_TRUE(accessor.addEmbeddings(&batch));
    }

    // For every query, compare the HNSW result set against exact ground truth and
    // accumulate how many of the true neighbours were recovered.
    size_t recoveredCount = 0;

    for (size_t q = 0; q < queryCount; q++) {
        const std::span<const float> queryVector(queries.data() + q * dimension, dimension);

        std::unordered_set<int64_t> expectedIds;
        exactTopK(dataset, vectorCount, queryVector, topK, expectedIds);

        VecLibAccessor accessor = db.getLibrary("hnsw_recall");
        ASSERT_TRUE(accessor.isValid());

        VectorSearchQuery query(dimension);
        query.setVector(queryVector);
        query.setMaxResultCount(topK);

        VectorSearchResult results;
        ASSERT_TRUE(accessor.search(&query, &results));

        for (const int64_t id : results.ids()) {
            if (expectedIds.contains(id)) {
                recoveredCount++;
            }
        }
    }

    const double recall = static_cast<double>(recoveredCount) / static_cast<double>(queryCount * topK);

    EXPECT_GE(recall, recallThreshold) << "HNSW recall@" << topK << " was " << recall;
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv);
}
