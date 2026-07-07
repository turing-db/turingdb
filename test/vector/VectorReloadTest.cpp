#include "TuringTest.h"

#include <array>

#include "VecLibAccessor.h"
#include "VectorDatabase.h"
#include "BatchVectorCreate.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"

using namespace vec;
using namespace turing::test;

class VectorReloadTest : public TuringTest {
public:
    void initialize() override {
        TuringTest::initialize();
        _rootDir = fs::Path {_outDir} / "vector_reload";

        if (!_rootDir.exists()) {
            ASSERT_TRUE(_rootDir.mkdir());
        }
    }

protected:
    fs::Path _rootDir;
};

// Exact vector search must still return results after the database is reloaded
// from disk. Adding embeddings registers shard signatures on the router; those
// signatures have to be persisted, otherwise an exact search after reload
// iterates an empty signature set and returns nothing.
TEST_F(VectorReloadTest, exactSearchAfterReload) {
    constexpr Dimension dimension = 8;

    const std::array<std::array<float, dimension>, 3> vectors {{
        {0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}, // id 0
        {1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}, // id 1
        {0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f}, // id 2
    }};

    const auto searchNearest = [&](VectorDatabase& db, int64_t& firstId, size_t& resultCount) {
        VecLibAccessor accessor = db.getLibrary("reloadlib");
        ASSERT_TRUE(accessor.isValid());

        VectorSearchResult results;
        VectorSearchQuery query(dimension);
        query.setVector(vectors[1]);
        query.setMaxResultCount(3);

        ASSERT_TRUE(accessor.search(&query, &results));

        resultCount = results.ids().size();
        firstId = resultCount > 0 ? results.ids()[0] : -1;
    };

    // Populate the database and persist it to disk.
    {
        VectorDatabase db;
        ASSERT_TRUE(db.init(_rootDir));

        const auto libRes = db.createLibrary("reloadlib", dimension, DistanceMetric::EUCLIDEAN_DIST);
        ASSERT_TRUE(libRes);

        VecLibAccessor accessor = db.getLibrary("reloadlib");
        ASSERT_TRUE(accessor.isValid());

        BatchVectorCreate batch;
        accessor.prepareCreateBatch(&batch);
        for (size_t i = 0; i < vectors.size(); i++) {
            batch.addPoint(static_cast<int64_t>(i), vectors[i]);
        }

        ASSERT_TRUE(accessor.addEmbeddings(&batch));

        // Sanity check: exact search works before any reload.
        int64_t firstId = -1;
        size_t resultCount = 0;
        searchNearest(db, firstId, resultCount);
        ASSERT_GT(resultCount, 0u);
        ASSERT_EQ(firstId, 1);
    }

    // Reload into a fresh instance (simulates a server restart) and search again.
    {
        VectorDatabase db;
        ASSERT_TRUE(db.init(_rootDir));

        int64_t firstId = -1;
        size_t resultCount = 0;
        searchNearest(db, firstId, resultCount);
        ASSERT_GT(resultCount, 0u);
        ASSERT_EQ(firstId, 1);
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv);
}
