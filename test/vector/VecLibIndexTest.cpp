#include "TuringTest.h"

#include <array>
#include <span>
#include <string_view>
#include <vector>

#include "VecLibAccessor.h"
#include "VectorDatabase.h"
#include "BatchVectorCreate.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"
#include "VectorException.h"

using namespace vec;
using namespace turing::test;

// Exercises the two VecLib index backends (FLAT exact search over LSH shards and
// approximate HNSW) end to end through the VectorDatabase API: add embeddings,
// then search and check the returned neighbours are ranked as expected under both
// distance metrics.
class VecLibIndexTest : public TuringTest {
public:
    void initialize() override {
        TuringTest::initialize();
        _rootDir = fs::Path {_outDir} / "veclib_index";

        if (!_rootDir.exists()) {
            ASSERT_TRUE(_rootDir.mkdir());
        }
    }

protected:
    static constexpr Dimension dimension = 4;

    // Five well-separated points. Ids 0-3 sit on the four axes; id 4 lies close to
    // id 0, so a query at id 0's position ranks id 0 first and id 4 second under
    // both euclidean distance and inner product.
    const std::array<std::array<float, dimension>, 5> _vectors {{
        {1.0f, 0.0f, 0.0f, 0.0f}, // id 0
        {0.0f, 1.0f, 0.0f, 0.0f}, // id 1
        {0.0f, 0.0f, 1.0f, 0.0f}, // id 2
        {0.0f, 0.0f, 0.0f, 1.0f}, // id 3
        {0.9f, 0.1f, 0.0f, 0.0f}, // id 4 (near id 0)
    }};

    fs::Path _rootDir;

    void createAndPopulate(VectorDatabase& db,
                           std::string_view libName,
                           DistanceMetric metric,
                           IndexType indexType) {
        const auto libRes = db.createLibrary(libName, dimension, metric, indexType);
        ASSERT_TRUE(libRes);

        VecLibAccessor accessor = db.getLibrary(libName);
        ASSERT_TRUE(accessor.isValid());

        BatchVectorCreate batch;
        accessor.prepareCreateBatch(&batch);

        for (size_t i = 0; i < _vectors.size(); i++) {
            batch.addPoint(static_cast<int64_t>(i), _vectors[i]);
        }

        ASSERT_TRUE(accessor.addEmbeddings(&batch));
    }

    void searchTopK(VectorDatabase& db,
                    std::string_view libName,
                    std::span<const float> queryVector,
                    size_t maxResults,
                    std::vector<int64_t>& outIds,
                    std::vector<float>& outDistances) {
        VecLibAccessor accessor = db.getLibrary(libName);
        ASSERT_TRUE(accessor.isValid());

        VectorSearchQuery query(dimension);
        query.setVector(queryVector);
        query.setMaxResultCount(maxResults);

        VectorSearchResult results;
        ASSERT_TRUE(accessor.search(&query, &results));

        const std::span<const int64_t> ids = results.ids();
        const std::span<const float> distances = results.distances();

        outIds.assign(ids.begin(), ids.end());
        outDistances.assign(distances.begin(), distances.end());
    }
};

// FLAT + euclidean: an exact query on a stored point returns that point first with
// a near-zero squared distance.
TEST_F(VecLibIndexTest, flatEuclideanReturnsExactMatch) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    createAndPopulate(db, "flat_l2", DistanceMetric::EUCLIDEAN_DIST, IndexType::FLAT);

    std::vector<int64_t> ids;
    std::vector<float> distances;
    searchTopK(db, "flat_l2", _vectors[0], 1, ids, distances);

    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 0);
    EXPECT_NEAR(distances[0], 0.0f, 1e-4f);
}

// FLAT + euclidean: the two nearest neighbours to id 0's position are id 0 then id 4.
TEST_F(VecLibIndexTest, flatEuclideanRanksNearestNeighbours) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    createAndPopulate(db, "flat_l2", DistanceMetric::EUCLIDEAN_DIST, IndexType::FLAT);

    std::vector<int64_t> ids;
    std::vector<float> distances;
    searchTopK(db, "flat_l2", _vectors[0], 2, ids, distances);

    ASSERT_EQ(ids.size(), 2u);
    EXPECT_EQ(ids[0], 0);
    EXPECT_EQ(ids[1], 4);
}

// FLAT + inner product: higher similarity ranks first, so id 0 (IP 1.0) precedes
// id 4 (IP 0.9).
TEST_F(VecLibIndexTest, flatInnerProductRanksBySimilarity) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    createAndPopulate(db, "flat_ip", DistanceMetric::INNER_PRODUCT, IndexType::FLAT);

    std::vector<int64_t> ids;
    std::vector<float> distances;
    searchTopK(db, "flat_ip", _vectors[0], 2, ids, distances);

    ASSERT_EQ(ids.size(), 2u);
    EXPECT_EQ(ids[0], 0);
    EXPECT_EQ(ids[1], 4);
    EXPECT_NEAR(distances[0], 1.0f, 1e-4f);
}

// FLAT: searching a freshly created, empty library returns no results (the search
// iterates an empty instantiated-shard set).
TEST_F(VecLibIndexTest, flatEmptyIndexReturnsNoResults) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    ASSERT_TRUE(db.createLibrary("flat_empty", dimension, DistanceMetric::EUCLIDEAN_DIST, IndexType::FLAT));

    std::vector<int64_t> ids;
    std::vector<float> distances;
    searchTopK(db, "flat_empty", _vectors[0], 3, ids, distances);

    EXPECT_TRUE(ids.empty());
}

// HNSW + euclidean: an exact query on a stored point returns that point first with
// a near-zero squared distance.
TEST_F(VecLibIndexTest, hnswEuclideanReturnsExactMatch) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    createAndPopulate(db, "hnsw_l2", DistanceMetric::EUCLIDEAN_DIST, IndexType::HNSW);

    std::vector<int64_t> ids;
    std::vector<float> distances;
    searchTopK(db, "hnsw_l2", _vectors[0], 1, ids, distances);

    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 0);
    EXPECT_NEAR(distances[0], 0.0f, 1e-4f);
}

// HNSW + euclidean: on this small index the approximate search still recovers the
// exact two nearest neighbours, id 0 then id 4.
TEST_F(VecLibIndexTest, hnswEuclideanRanksNearestNeighbours) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    createAndPopulate(db, "hnsw_l2", DistanceMetric::EUCLIDEAN_DIST, IndexType::HNSW);

    std::vector<int64_t> ids;
    std::vector<float> distances;
    searchTopK(db, "hnsw_l2", _vectors[0], 2, ids, distances);

    ASSERT_EQ(ids.size(), 2u);
    EXPECT_EQ(ids[0], 0);
    EXPECT_EQ(ids[1], 4);
}

// HNSW + inner product: higher similarity ranks first, so id 0 (IP 1.0) precedes
// id 4 (IP 0.9).
TEST_F(VecLibIndexTest, hnswInnerProductRanksBySimilarity) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    createAndPopulate(db, "hnsw_ip", DistanceMetric::INNER_PRODUCT, IndexType::HNSW);

    std::vector<int64_t> ids;
    std::vector<float> distances;
    searchTopK(db, "hnsw_ip", _vectors[0], 2, ids, distances);

    ASSERT_EQ(ids.size(), 2u);
    EXPECT_EQ(ids[0], 0);
    EXPECT_EQ(ids[1], 4);
    EXPECT_NEAR(distances[0], 1.0f, 1e-4f);
}

// HNSW: searching a freshly created, empty library returns no results (the search
// is guarded on a non-empty index).
TEST_F(VecLibIndexTest, hnswEmptyIndexReturnsNoResults) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    ASSERT_TRUE(db.createLibrary("hnsw_empty", dimension, DistanceMetric::EUCLIDEAN_DIST, IndexType::HNSW));

    std::vector<int64_t> ids;
    std::vector<float> distances;
    searchTopK(db, "hnsw_empty", _vectors[0], 3, ids, distances);

    EXPECT_TRUE(ids.empty());
}

// Adding a point whose dimension does not match the library rejects the input with
// a VectorException rather than silently ingesting a malformed vector.
TEST_F(VecLibIndexTest, addPointDimensionMismatchThrows) {
    VectorDatabase db;
    ASSERT_TRUE(db.init(_rootDir));

    ASSERT_TRUE(db.createLibrary("mismatch", dimension, DistanceMetric::EUCLIDEAN_DIST, IndexType::FLAT));

    VecLibAccessor accessor = db.getLibrary("mismatch");
    ASSERT_TRUE(accessor.isValid());

    BatchVectorCreate batch;
    accessor.prepareCreateBatch(&batch);

    const std::array<float, dimension + 1> wrongDimension {1.0f, 0.0f, 0.0f, 0.0f, 0.0f};
    EXPECT_THROW(batch.addPoint(0, wrongDimension), VectorException);
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv);
}
