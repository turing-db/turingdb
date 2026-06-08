#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <vector>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "SystemManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "versioning/Change.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

namespace {

// Helper struct to hold a vector with its ID
struct TestVector {
    int64_t _id {0};
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
void findKNearestNeighbors(std::vector<int64_t>& result,
                           const std::vector<TestVector>& vectors,
                           const std::vector<float>& query,
                           size_t k) {
    // Compute distances for all vectors
    std::vector<std::pair<float, int64_t>> distances;
    for (const auto& vec : vectors) {
        const float dist = euclideanDistanceSquared(vec.values, query);
        distances.emplace_back(dist, vec._id);
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
    auto query(std::string_view q, std::string_view graphName, auto callback,
               db::ChangeID change = db::ChangeID::head()) {
        db::QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const db::QueryState state(graphName, &_env->getMem(), &_queryConfig, &callbacks, db::CommitHash::head(), change);
        return _db->query(q, state);
    }

    void initialize() override {
        const auto testTuringDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(testTuringDir);
        _db = &_env->getDB();
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(VectorQueriesTest, createVectorIndex) {
    bool executed = false;
    const auto res = query("CREATE VECTOR INDEX embeddings WITH DIMENSION 128 METRIC EUCLID", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getLogicalRowCount(), 1);

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
    const auto showRes = query("SHOW VECTOR INDEXES", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_GE(df->getLogicalRowCount(), 1);

            // Check that "embeddings" is in the list
            const auto& cols = df->cols();
            ASSERT_GE(cols.size(), 1);

            // Find the name column
            const auto* nameCol = cols.at(0)->as<ColumnVector<types::String::Primitive>>();
            ASSERT_TRUE(nameCol != nullptr);

            bool found = false;
            for (size_t i = 0; i < df->getLogicalRowCount(); ++i) {
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
    auto createRes = query("CREATE VECTOR INDEX test_index WITH DIMENSION 64 METRIC COSINE", "default", [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Then show indexes
    bool executed = false;
    const auto res = query("SHOW VECTOR INDEXES", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 2);  // name and dimension columns
            ASSERT_GE(df->getLogicalRowCount(), 1);

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
            for (size_t i = 0; i < df->getLogicalRowCount(); ++i) {
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
    auto createRes = query("CREATE VECTOR INDEX to_delete WITH DIMENSION 32 METRIC EUCLID", "default", [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Then delete it
    bool executed = false;
    const auto res = query("DELETE VECTOR INDEX to_delete", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getLogicalRowCount(), 1);

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
    const auto showRes = query("SHOW VECTOR INDEXES", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);

            // Check that "to_delete" is NOT in the list
            if (df->getLogicalRowCount() > 0) {
                const auto& cols = df->cols();
                const auto* nameCol = cols.at(0)->as<ColumnVector<types::String::Primitive>>();
                ASSERT_TRUE(nameCol != nullptr);

                for (size_t i = 0; i < df->getLogicalRowCount(); ++i) {
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
    const auto res = query("CREATE VECTOR INDEX cosine_index WITH DIMENSION 256 METRIC COSINE", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getLogicalRowCount(), 1);

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
    const auto showRes = query("SHOW VECTOR INDEXES", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);

            const auto& cols = df->cols();
            const auto* nameCol = cols.at(0)->as<ColumnVector<types::String::Primitive>>();
            const auto* dimCol = cols.at(1)->as<ColumnVector<types::UInt64::Primitive>>();
            ASSERT_TRUE(nameCol != nullptr);
            ASSERT_TRUE(dimCol != nullptr);

            bool found = false;
            for (size_t i = 0; i < df->getLogicalRowCount(); ++i) {
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
    auto createRes = query("CREATE VECTOR INDEX load_test WITH DIMENSION 4 METRIC EUCLID", "default", [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Create a temporary CSV file with test vectors in the data directory
    // Format: id,dim1,dim2,dim3,dim4
    std::string dataDir = _outDir + "/turing/data";
    std::string vectorFile = dataDir + "/test_vectors.csv";
    {
        std::ofstream out(vectorFile);
        out << "1,1.0,0.0,0.0,0.0\n";
        out << "2,0.0,1.0,0.0,0.0\n";
        out << "3,0.0,0.0,1.0,0.0\n";
        out << "4,0.0,0.0,0.0,1.0\n";
        out << "5,0.5,0.5,0.0,0.0\n";
    }

    // Load vectors from file (path relative to data directory)
    bool executed = false;
    std::string loadQuery = "LOAD VECTOR FROM \"test_vectors.csv\" IN load_test";
    const auto res = query(loadQuery, "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getLogicalRowCount(), 1);

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
    auto createRes = query("CREATE VECTOR INDEX search_test WITH DIMENSION 4 METRIC EUCLID", "default", [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Define test vectors
    std::vector<TestVector> testVectors = {
        {1, {1.0f, 0.0f, 0.0f, 0.0f}},   // unit vector along x-axis
        {2, {0.0f, 1.0f, 0.0f, 0.0f}},   // unit vector along y-axis
        {3, {0.0f, 0.0f, 1.0f, 0.0f}},   // unit vector along z-axis
        {4, {0.9f, 0.1f, 0.0f, 0.0f}},   // close to vector 1
        {5, {0.8f, 0.2f, 0.0f, 0.0f}},   // also close to vector 1
    };

    // Write vectors to CSV file in the data directory
    std::string dataDir = _outDir + "/turing/data";
    std::string vectorFile = dataDir + "/search_vectors.csv";
    {
        std::ofstream out(vectorFile);
        for (const auto& vec : testVectors) {
            out << vec._id;
            for (float v : vec.values) {
                out << "," << v;
            }
            out << "\n";
        }
    }

    // Load vectors (path relative to data directory)
    std::string loadQuery = "LOAD VECTOR FROM \"search_vectors.csv\" IN search_test";
    auto loadRes = query(loadQuery, "default", [](const Dataframe*) {});
    ASSERT_TRUE(loadRes.isOk()) << "Load failed: " << loadRes.getError();

    // Define query vector and compute expected results
    std::vector<float> queryVector = {1.0f, 0.0f, 0.0f, 0.0f};
    const size_t k = 3;
    std::vector<int64_t> expectedIds;
    findKNearestNeighbors(expectedIds, testVectors, queryVector, k);

    // Search for vectors closest to query
    bool executed = false;
    const auto res = query("VECTOR SEARCH IN search_test FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getLogicalRowCount(), k);

            const auto& cols = df->cols();
            const auto* colIds = cols.at(0)->as<ColumnVector<types::Int64::Primitive>>();
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
    auto createRes = query("CREATE VECTOR INDEX k_test WITH DIMENSION 4 METRIC EUCLID", "default", [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Define test vectors
    std::vector<TestVector> testVectors = {
        {10, {1.0f, 0.0f, 0.0f, 0.0f}},
        {20, {0.9f, 0.1f, 0.0f, 0.0f}},
        {30, {0.8f, 0.2f, 0.0f, 0.0f}},
        {40, {0.7f, 0.3f, 0.0f, 0.0f}},
        {50, {0.6f, 0.4f, 0.0f, 0.0f}},
    };

    // Write vectors to CSV file in the data directory
    std::string dataDir = _outDir + "/turing/data";
    std::string vectorFile = dataDir + "/k_test_vectors.csv";
    {
        std::ofstream out(vectorFile);
        for (const auto& vec : testVectors) {
            out << vec._id;
            for (float v : vec.values) {
                out << "," << v;
            }
            out << "\n";
        }
    }

    // Load vectors (path relative to data directory)
    std::string loadQuery = "LOAD VECTOR FROM \"k_test_vectors.csv\" IN k_test";
    auto loadRes = query(loadQuery, "default", [](const Dataframe*) {});
    ASSERT_TRUE(loadRes.isOk()) << "Load failed: " << loadRes.getError();

    // Define query vector and compute expected results for k=2
    std::vector<float> queryVector = {1.0f, 0.0f, 0.0f, 0.0f};
    const size_t k = 2;
    std::vector<int64_t> expectedIds;
    findKNearestNeighbors(expectedIds, testVectors, queryVector, k);

    // Search for top 2
    bool executed = false;
    const auto res = query("VECTOR SEARCH IN k_test FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->getLogicalRowCount(), k);

            const auto& cols = df->cols();
            const auto* colIds = cols.at(0)->as<ColumnVector<types::Int64::Primitive>>();
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
    auto createRes = query("CREATE VECTOR INDEX precision_test WITH DIMENSION 4 METRIC EUCLID", "default", [](const Dataframe*) {});
    ASSERT_TRUE(createRes.isOk()) << "Create failed: " << createRes.getError();

    // Define test vectors with high-precision floating point values (6 decimals)
    std::vector<TestVector> testVectors = {
        {100, {0.123456f, 0.678901f, 0.111111f, 0.222222f}},
        {200, {0.123467f, 0.678912f, 0.111122f, 0.222233f}},  // very close to 100
        {300, {0.987654f, 0.432109f, 0.555555f, 0.666666f}},  // far from 100
        {400, {0.123445f, 0.678890f, 0.111100f, 0.222211f}},  // very close to 100
        {500, {0.543210f, 0.098765f, 0.333333f, 0.444444f}},  // medium distance
    };

    // Write vectors to CSV file with high precision in the data directory
    std::string dataDir = _outDir + "/turing/data";
    std::string vectorFile = dataDir + "/precision_vectors.csv";
    {
        std::ofstream out(vectorFile);
        out << std::fixed << std::setprecision(6);
        for (const auto& vec : testVectors) {
            out << vec._id;
            for (float v : vec.values) {
                out << "," << v;
            }
            out << "\n";
        }
    }

    // Load vectors (path relative to data directory)
    std::string loadQuery = "LOAD VECTOR FROM \"precision_vectors.csv\" IN precision_test";
    auto loadRes = query(loadQuery, "default", [](const Dataframe*) {});
    ASSERT_TRUE(loadRes.isOk()) << "Load failed: " << loadRes.getError();

    // Define query vector (matches vector 100 exactly) and compute expected results
    std::vector<float> queryVector = {0.123456f, 0.678901f, 0.111111f, 0.222222f};
    const size_t k = 3;
    std::vector<int64_t> expectedIds;
    findKNearestNeighbors(expectedIds, testVectors, queryVector, k);

    // Search for vectors closest to query
    bool executed = false;
    const auto res = query("VECTOR SEARCH IN precision_test FOR 3 (0.123456, 0.678901, 0.111111, 0.222222) "
        "YIELD ids RETURN ids", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getLogicalRowCount(), k);

            const auto& cols = df->cols();
            const auto* colIds = cols.at(0)->as<ColumnVector<types::Int64::Primitive>>();
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

TEST_F(VectorQueriesTest, vectorSearchWithMatch) {
    // This test verifies that VECTOR SEARCH combined with MATCH correctly
    // retrieves graph node properties for the k nearest neighbors.

    // Step 1: Create nodes with id and title properties
    // We need to use change management for write operations
    {
        ChangeID changeId;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto changeRes = system.newChange("default");
            ASSERT_TRUE(changeRes) << "Failed to create change";
            changeId = changeRes.value()->id();
        }

        const auto createRes = query(R"(CREATE (n1:Document {id: 1, title: "Doc One"}),
                      (n2:Document {id: 2, title: "Doc Two"}),
                      (n3:Document {id: 3, title: "Doc Three"}),
                      (n4:Document {id: 4, title: "Doc Four"}),
                      (n5:Document {id: 5, title: "Doc Five"}))", "default", [](const Dataframe*) {}, changeId);
        ASSERT_TRUE(createRes.isOk()) << "CREATE nodes failed: " << createRes.getError();

        const auto commitRes = query("change submit", "default", [](const Dataframe*) {}, changeId);
        ASSERT_TRUE(commitRes.isOk()) << "COMMIT failed: " << commitRes.getError();
    }

    // Step 1b: Verify nodes were created
    {
        size_t nodeCount = 0;
        const auto matchRes = query("MATCH (n:Document) RETURN n.id", "default", [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                nodeCount = df->getLogicalRowCount();
            });
        ASSERT_TRUE(matchRes.isOk()) << "MATCH query failed: " << matchRes.getError();
        ASSERT_EQ(nodeCount, 5) << "Expected 5 Document nodes";
    }

    // Step 2: Create vector index
    {
        const auto createIndexRes = query("CREATE VECTOR INDEX doc_vectors WITH DIMENSION 4 METRIC EUCLID", "default", [](const Dataframe*) {});
        ASSERT_TRUE(createIndexRes.isOk())
            << "Create index failed: " << createIndexRes.getError();
    }

    // Step 3: Create and load test vectors
    // Design vectors for predictable k-NN results with query [1.0, 0.0, 0.0, 0.0]:
    // ID 1: distance = 0 (exact match)
    // ID 2: distance = 0.02 (second closest)
    // ID 3: distance = 0.08 (third closest)
    // ID 4: distance = 2.0 (far)
    // ID 5: distance = 2.0 (far)
    const std::vector<TestVector> testVectors = {
        {1, {1.0f, 0.0f, 0.0f, 0.0f}},
        {2, {0.9f, 0.1f, 0.0f, 0.0f}},
        {3, {0.8f, 0.2f, 0.0f, 0.0f}},
        {4, {0.0f, 1.0f, 0.0f, 0.0f}},
        {5, {0.0f, 0.0f, 1.0f, 0.0f}},
    };

    const std::string dataDir = _outDir + "/turing/data";
    const std::string vectorFile = dataDir + "/doc_vectors.csv";
    {
        std::ofstream out(vectorFile);
        for (const auto& vec : testVectors) {
            out << vec._id;
            for (const float v : vec.values) {
                out << "," << v;
            }
            out << "\n";
        }
    }

    const std::string loadQuery = "LOAD VECTOR FROM \"doc_vectors.csv\" IN doc_vectors";
    const auto loadRes =
        query(loadQuery, "default", [](const Dataframe*) {});
    ASSERT_TRUE(loadRes.isOk()) << "Load vectors failed: " << loadRes.getError();

    // Step 3b: Verify vector search works standalone
    {
        const auto searchRes = query("VECTOR SEARCH IN doc_vectors FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids", "default", [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->getLogicalRowCount(), 3) << "Expected 3 vector search results";
            });
        ASSERT_TRUE(searchRes.isOk()) << "Vector search failed: " << searchRes.getError();
    }

    // Step 4: Execute combined VECTOR SEARCH + MATCH query
    const std::vector<float> queryVector = {1.0f, 0.0f, 0.0f, 0.0f};
    const size_t k = 3;

    // Compute expected results using helper function
    std::vector<int64_t> expectedIds;
    findKNearestNeighbors(expectedIds, testVectors, queryVector, k);

    // Expected titles corresponding to the expected IDs
    const std::vector<std::string> expectedTitles = {"Doc One", "Doc Two", "Doc Three"};

    bool executed = false;
    const auto res = query("VECTOR SEARCH IN doc_vectors FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids "
        "MATCH (n:Document) WHERE n.id = ids "
        "RETURN n.id, n.title", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 2) << "Expected 2 columns (n.id, n.title)";
            ASSERT_EQ(df->getLogicalRowCount(), k) << "Expected " << k << " rows";

            const auto& cols = df->cols();

            // Verify column names
            ASSERT_EQ(cols.at(0)->getName(), "n.id");
            ASSERT_EQ(cols.at(1)->getName(), "n.title");

            // Get columns - properties return ColumnOptVector (nullable)
            const auto* idCol =
                cols.at(0)->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* titleCol =
                cols.at(1)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(idCol != nullptr) << "Failed to cast n.id column";
            ASSERT_TRUE(titleCol != nullptr) << "Failed to cast n.title column";

            // Verify each row has expected values
            for (size_t i = 0; i < k; ++i) {
                // Check id value
                ASSERT_TRUE(idCol->at(i).has_value())
                    << "n.id is null at row " << i;
                ASSERT_EQ(*idCol->at(i), expectedIds[i])
                    << "ID mismatch at row " << i << ": expected " << expectedIds[i]
                    << ", got " << *idCol->at(i);

                // Check title value
                ASSERT_TRUE(titleCol->at(i).has_value())
                    << "n.title is null at row " << i;
                ASSERT_EQ(*titleCol->at(i), expectedTitles[i])
                    << "Title mismatch at row " << i << ": expected " << expectedTitles[i]
                    << ", got " << *titleCol->at(i);
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
