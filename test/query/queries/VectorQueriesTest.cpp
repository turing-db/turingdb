#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <vector>

#include "TuringDB.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

namespace {

// Helper struct to hold a vector with its ID
struct TestVector {
    uint64_t id {0};
    std::vector<float> values;
};

// Compute squared Euclidean distance between two vectors
float euclideanDistanceSquared(const std::vector<float>& a, const std::vector<float>& b) {
    float dist = 0.0f;
    for (size_t i = 0; i < a.size(); ++i) {
        const float diff = a[i] - b[i];
        dist += diff * diff;
    }

    return dist;
}

// Find k nearest neighbors using Euclidean distance
void findKNearestNeighbors(std::vector<uint64_t>& result,
                           const std::vector<TestVector>& vectors,
                           const std::vector<float>& query,
                           size_t k) {
    // Compute distances for all vectors
    std::vector<std::pair<float, uint64_t>> distances;
    for (const auto& vec : vectors) {
        const float dist = euclideanDistanceSquared(vec.values, query);
        distances.emplace_back(dist, vec.id);
    }

    // Sort by distance (ascending)
    std::sort(distances.begin(), distances.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // Extract top k IDs
    result.clear();
    for (size_t i = 0; i < k && i < distances.size(); ++i) {
        result.push_back(distances[i].second);
    }
}

}  // namespace

class VectorQueriesTest : public TuringTest {
public:
    void initialize() override {
        const auto testTuringDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(testTuringDir);
        _db = &_env->getDB();
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
};

TEST_F(VectorQueriesTest, createVectorIndex) {
    bool executed = false;
    const auto res = _db->query(
        "CREATE VECTOR INDEX embeddings WITH DIMENSION 128 METRIC EUCLID",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getRowCount(), 1);

            const auto& cols = df->cols();

            // Check the column name is "indexName"
            ASSERT_EQ(cols.at(0)->getName(), "indexName");

            // Check the column value is the index name we created
            const auto* colName = cols.at(0)->as<ColumnConst<types::String::Primitive>>();
            ASSERT_TRUE(colName != nullptr);
            ASSERT_EQ(colName->getRaw(), "embeddings");

            executed = true;
        });

    ASSERT_TRUE(res.isOk()) << "Query failed: " << res.getError();
    ASSERT_TRUE(executed);

    // Verify the index was actually created by showing indexes
    bool showExecuted = false;
    const auto showRes = _db->query(
        "SHOW VECTOR INDEXES",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_GE(df->getRowCount(), 1);

            // Check that "embeddings" is in the list
            const auto& cols = df->cols();
            ASSERT_GE(cols.size(), 1);

            // Find the name column
            const auto* nameCol = cols.at(0)->as<ColumnVector<types::String::Primitive>>();
            ASSERT_TRUE(nameCol != nullptr);

            bool found = false;
            for (size_t i = 0; i < df->getRowCount(); ++i) {
                if (nameCol->at(i) == "embeddings") {
                    found = true;
                    break;
                }
            }
            ASSERT_TRUE(found) << "Index 'embeddings' not found in SHOW VECTOR INDEXES";

            showExecuted = true;
        });

    ASSERT_TRUE(showRes.isOk()) << "SHOW VECTOR INDEXES failed: " << showRes.getError();
    ASSERT_TRUE(showExecuted);
}

TEST_F(VectorQueriesTest, showVectorIndexes) {
    // First create an index
    auto createRes = _db->query(
        "CREATE VECTOR INDEX test_index WITH DIMENSION 64 METRIC COSINE",
        "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Then show indexes
    bool executed = false;
    const auto res = _db->query(
        "SHOW VECTOR INDEXES",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 2);  // name and dimension columns
            ASSERT_GE(df->getRowCount(), 1);

            const auto& cols = df->cols();

            // Check column names
            ASSERT_EQ(cols.at(0)->getName(), "name");
            ASSERT_EQ(cols.at(1)->getName(), "dimension");

            // Check column types and find our index
            const auto* nameCol = cols.at(0)->as<ColumnVector<types::String::Primitive>>();
            const auto* dimCol = cols.at(1)->as<ColumnVector<types::UInt64::Primitive>>();
            ASSERT_TRUE(nameCol != nullptr);
            ASSERT_TRUE(dimCol != nullptr);

            // Find "test_index" and verify its dimension
            bool found = false;
            for (size_t i = 0; i < df->getRowCount(); ++i) {
                if (nameCol->at(i) == "test_index") {
                    ASSERT_EQ(dimCol->at(i), 64) << "Dimension mismatch for test_index";
                    found = true;
                    break;
                }
            }
            ASSERT_TRUE(found) << "Index 'test_index' not found in SHOW VECTOR INDEXES";

            executed = true;
        });

    ASSERT_TRUE(res.isOk()) << "Query failed: " << res.getError();
    ASSERT_TRUE(executed);
}

TEST_F(VectorQueriesTest, deleteVectorIndex) {
    // First create an index
    auto createRes = _db->query(
        "CREATE VECTOR INDEX to_delete WITH DIMENSION 32 METRIC EUCLID",
        "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Then delete it
    bool executed = false;
    const auto res = _db->query(
        "DELETE VECTOR INDEX to_delete",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getRowCount(), 1);

            const auto& cols = df->cols();

            // Check column name
            ASSERT_EQ(cols.at(0)->getName(), "indexName");

            // Check column value
            const auto* colName = cols.at(0)->as<ColumnConst<types::String::Primitive>>();
            ASSERT_TRUE(colName != nullptr);
            ASSERT_EQ(colName->getRaw(), "to_delete");

            executed = true;
        });

    ASSERT_TRUE(res.isOk()) << "Query failed: " << res.getError();
    ASSERT_TRUE(executed);

    // Verify the index was actually deleted by showing indexes
    bool showExecuted = false;
    const auto showRes = _db->query(
        "SHOW VECTOR INDEXES",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);

            // Check that "to_delete" is NOT in the list
            if (df->getRowCount() > 0) {
                const auto& cols = df->cols();
                const auto* nameCol = cols.at(0)->as<ColumnVector<types::String::Primitive>>();
                ASSERT_TRUE(nameCol != nullptr);

                for (size_t i = 0; i < df->getRowCount(); ++i) {
                    ASSERT_NE(nameCol->at(i), "to_delete")
                        << "Index 'to_delete' should have been deleted";
                }
            }

            showExecuted = true;
        });

    ASSERT_TRUE(showRes.isOk()) << "SHOW VECTOR INDEXES failed: " << showRes.getError();
    ASSERT_TRUE(showExecuted);
}

TEST_F(VectorQueriesTest, createVectorIndexWithCosineMetric) {
    bool executed = false;
    const auto res = _db->query(
        "CREATE VECTOR INDEX cosine_index WITH DIMENSION 256 METRIC COSINE",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getRowCount(), 1);

            const auto& cols = df->cols();

            // Check column name
            ASSERT_EQ(cols.at(0)->getName(), "indexName");

            // Check the column value
            const auto* colName = cols.at(0)->as<ColumnConst<types::String::Primitive>>();
            ASSERT_TRUE(colName != nullptr);
            ASSERT_EQ(colName->getRaw(), "cosine_index");

            executed = true;
        });

    ASSERT_TRUE(res.isOk()) << "Query failed: " << res.getError();
    ASSERT_TRUE(executed);

    // Verify the index was created with COSINE metric by showing indexes
    bool showExecuted = false;
    const auto showRes = _db->query(
        "SHOW VECTOR INDEXES",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);

            const auto& cols = df->cols();
            const auto* nameCol = cols.at(0)->as<ColumnVector<types::String::Primitive>>();
            const auto* dimCol = cols.at(1)->as<ColumnVector<types::UInt64::Primitive>>();
            ASSERT_TRUE(nameCol != nullptr);
            ASSERT_TRUE(dimCol != nullptr);

            bool found = false;
            for (size_t i = 0; i < df->getRowCount(); ++i) {
                if (nameCol->at(i) == "cosine_index") {
                    ASSERT_EQ(dimCol->at(i), 256) << "Dimension mismatch for cosine_index";
                    found = true;
                    break;
                }
            }
            ASSERT_TRUE(found) << "Index 'cosine_index' not found in SHOW VECTOR INDEXES";

            showExecuted = true;
        });

    ASSERT_TRUE(showRes.isOk()) << "SHOW VECTOR INDEXES failed: " << showRes.getError();
    ASSERT_TRUE(showExecuted);
}

TEST_F(VectorQueriesTest, loadVectorFromFile) {
    // Create a vector index with dimension 4
    auto createRes = _db->query(
        "CREATE VECTOR INDEX load_test WITH DIMENSION 4 METRIC EUCLID",
        "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Create a temporary CSV file with test vectors
    // Format: id,dim1,dim2,dim3,dim4
    std::string vectorFile = _outDir + "/test_vectors.csv";
    {
        std::ofstream out(vectorFile);
        out << "1,1.0,0.0,0.0,0.0\n";
        out << "2,0.0,1.0,0.0,0.0\n";
        out << "3,0.0,0.0,1.0,0.0\n";
        out << "4,0.0,0.0,0.0,1.0\n";
        out << "5,0.5,0.5,0.0,0.0\n";
    }

    // Load vectors from file
    bool executed = false;
    std::string loadQuery = "LOAD VECTOR FROM \"" + vectorFile + "\" IN load_test";
    const auto res = _db->query(
        loadQuery,
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getRowCount(), 1);

            const auto& cols = df->cols();

            // Check column name
            ASSERT_EQ(cols.at(0)->getName(), "count");

            // Check the count of loaded vectors
            const auto* colCount = cols.at(0)->as<ColumnConst<types::UInt64::Primitive>>();
            ASSERT_TRUE(colCount != nullptr);
            ASSERT_EQ(colCount->getRaw(), 5);  // We loaded 5 vectors

            executed = true;
        });

    ASSERT_TRUE(res.isOk()) << "Query failed: " << res.getError();
    ASSERT_TRUE(executed);
}

TEST_F(VectorQueriesTest, vectorSearchReturnsCorrectResults) {
    // Create a vector index with dimension 4
    auto createRes = _db->query(
        "CREATE VECTOR INDEX search_test WITH DIMENSION 4 METRIC EUCLID",
        "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Define test vectors
    std::vector<TestVector> testVectors = {
        {1, {1.0f, 0.0f, 0.0f, 0.0f}},   // unit vector along x-axis
        {2, {0.0f, 1.0f, 0.0f, 0.0f}},   // unit vector along y-axis
        {3, {0.0f, 0.0f, 1.0f, 0.0f}},   // unit vector along z-axis
        {4, {0.9f, 0.1f, 0.0f, 0.0f}},   // close to vector 1
        {5, {0.8f, 0.2f, 0.0f, 0.0f}},   // also close to vector 1
    };

    // Write vectors to CSV file
    std::string vectorFile = _outDir + "/search_vectors.csv";
    {
        std::ofstream out(vectorFile);
        for (const auto& vec : testVectors) {
            out << vec.id;
            for (float v : vec.values) {
                out << "," << v;
            }
            out << "\n";
        }
    }

    // Load vectors
    std::string loadQuery = "LOAD VECTOR FROM \"" + vectorFile + "\" IN search_test";
    auto loadRes = _db->query(loadQuery, "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(loadRes.isOk()) << "Load failed: " << loadRes.getError();

    // Define query vector and compute expected results
    std::vector<float> queryVector = {1.0f, 0.0f, 0.0f, 0.0f};
    const size_t k = 3;
    std::vector<uint64_t> expectedIds;
    findKNearestNeighbors(expectedIds, testVectors, queryVector, k);

    // Search for vectors closest to query
    bool executed = false;
    const auto res = _db->query(
        "VECTOR SEARCH IN search_test FOR 3 [1.0, 0.0, 0.0, 0.0] YIELD ids RETURN ids",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getRowCount(), k);

            const auto& cols = df->cols();
            const auto* colIds = cols.at(0)->as<ColumnVector<types::UInt64::Primitive>>();
            ASSERT_TRUE(colIds != nullptr);

            // Verify results match expected nearest neighbors
            for (size_t i = 0; i < k; ++i) {
                ASSERT_EQ(colIds->at(i), expectedIds[i])
                    << "Mismatch at position " << i << ": expected " << expectedIds[i]
                    << ", got " << colIds->at(i);
            }

            executed = true;
        });

    ASSERT_TRUE(res.isOk()) << "Query failed: " << res.getError();
    ASSERT_TRUE(executed);
}

TEST_F(VectorQueriesTest, vectorSearchWithDifferentK) {
    // Create a vector index
    auto createRes = _db->query(
        "CREATE VECTOR INDEX k_test WITH DIMENSION 4 METRIC EUCLID",
        "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Define test vectors
    std::vector<TestVector> testVectors = {
        {10, {1.0f, 0.0f, 0.0f, 0.0f}},
        {20, {0.9f, 0.1f, 0.0f, 0.0f}},
        {30, {0.8f, 0.2f, 0.0f, 0.0f}},
        {40, {0.7f, 0.3f, 0.0f, 0.0f}},
        {50, {0.6f, 0.4f, 0.0f, 0.0f}},
    };

    // Write vectors to CSV file
    std::string vectorFile = _outDir + "/k_test_vectors.csv";
    {
        std::ofstream out(vectorFile);
        for (const auto& vec : testVectors) {
            out << vec.id;
            for (float v : vec.values) {
                out << "," << v;
            }
            out << "\n";
        }
    }

    // Load vectors
    std::string loadQuery = "LOAD VECTOR FROM \"" + vectorFile + "\" IN k_test";
    auto loadRes = _db->query(loadQuery, "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(loadRes.isOk()) << "Load failed: " << loadRes.getError();

    // Define query vector and compute expected results for k=2
    std::vector<float> queryVector = {1.0f, 0.0f, 0.0f, 0.0f};
    const size_t k = 2;
    std::vector<uint64_t> expectedIds;
    findKNearestNeighbors(expectedIds, testVectors, queryVector, k);

    // Search for top 2
    bool executed = false;
    const auto res = _db->query(
        "VECTOR SEARCH IN k_test FOR 2 [1.0, 0.0, 0.0, 0.0] YIELD ids RETURN ids",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->getRowCount(), k);

            const auto& cols = df->cols();
            const auto* colIds = cols.at(0)->as<ColumnVector<types::UInt64::Primitive>>();
            ASSERT_TRUE(colIds != nullptr);

            // Verify results match expected nearest neighbors
            for (size_t i = 0; i < k; ++i) {
                ASSERT_EQ(colIds->at(i), expectedIds[i])
                    << "Mismatch at position " << i << ": expected " << expectedIds[i]
                    << ", got " << colIds->at(i);
            }

            executed = true;
        });

    ASSERT_TRUE(res.isOk()) << "Query failed: " << res.getError();
    ASSERT_TRUE(executed);
}

TEST_F(VectorQueriesTest, vectorSearchWithHighPrecisionFloats) {
    // Create a vector index with dimension 4
    auto createRes = _db->query(
        "CREATE VECTOR INDEX precision_test WITH DIMENSION 4 METRIC EUCLID",
        "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Define test vectors with high-precision floating point values (6 decimals)
    std::vector<TestVector> testVectors = {
        {100, {0.123456f, 0.678901f, 0.111111f, 0.222222f}},
        {200, {0.123467f, 0.678912f, 0.111122f, 0.222233f}},  // very close to 100
        {300, {0.987654f, 0.432109f, 0.555555f, 0.666666f}},  // far from 100
        {400, {0.123445f, 0.678890f, 0.111100f, 0.222211f}},  // very close to 100
        {500, {0.543210f, 0.098765f, 0.333333f, 0.444444f}},  // medium distance
    };

    // Write vectors to CSV file with high precision
    std::string vectorFile = _outDir + "/precision_vectors.csv";
    {
        std::ofstream out(vectorFile);
        out << std::fixed << std::setprecision(6);
        for (const auto& vec : testVectors) {
            out << vec.id;
            for (float v : vec.values) {
                out << "," << v;
            }
            out << "\n";
        }
    }

    // Load vectors
    std::string loadQuery = "LOAD VECTOR FROM \"" + vectorFile + "\" IN precision_test";
    auto loadRes = _db->query(loadQuery, "default", &_env->getMem(), [](const Dataframe*) {});
    ASSERT_TRUE(loadRes.isOk()) << "Load failed: " << loadRes.getError();

    // Define query vector (matches vector 100 exactly) and compute expected results
    std::vector<float> queryVector = {0.123456f, 0.678901f, 0.111111f, 0.222222f};
    const size_t k = 3;
    std::vector<uint64_t> expectedIds;
    findKNearestNeighbors(expectedIds, testVectors, queryVector, k);

    // Search for vectors closest to query
    bool executed = false;
    const auto res = _db->query(
        "VECTOR SEARCH IN precision_test FOR 3 [0.123456, 0.678901, 0.111111, 0.222222] "
        "YIELD ids RETURN ids",
        "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getRowCount(), k);

            const auto& cols = df->cols();
            const auto* colIds = cols.at(0)->as<ColumnVector<types::UInt64::Primitive>>();
            ASSERT_TRUE(colIds != nullptr);

            // Verify results match expected nearest neighbors
            for (size_t i = 0; i < k; ++i) {
                ASSERT_EQ(colIds->at(i), expectedIds[i])
                    << "Mismatch at position " << i << ": expected " << expectedIds[i]
                    << ", got " << colIds->at(i);
            }

            executed = true;
        });

    ASSERT_TRUE(res.isOk()) << "Query failed: " << res.getError();
    ASSERT_TRUE(executed);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
