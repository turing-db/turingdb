#include "TuringTest.h"

#include "VecLibAccessor.h"
#include "VectorDatabase.h"
#include "BatchVectorCreate.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"
#include "import/JsonImporter.h"

using namespace vec;
using namespace turing::test;

class JsonImporterTest : public TuringTest {
public:
    void initialize() override {
        TuringTest::initialize();
        _rootDir = fs::Path {_outDir} / "vector";

        if (!_rootDir.exists()) {
            ASSERT_TRUE(_rootDir.mkdir());
        }

        {
            auto res = VectorDatabase::create(_rootDir);
            ASSERT_TRUE(res);
            _db = std::move(res.value());
        }

        {
            auto res = _db->createLibrary("mylib", 8, DistanceMetric::INNER_PRODUCT);
            ASSERT_TRUE(res);
            _mylibID = res.value();
        }
    }

protected:
    std::unique_ptr<VectorDatabase> _db;
    VecLibID _mylibID {0};
    fs::Path _rootDir;
};

#define INVALID_JSON(jsonstr)                           \
    {                                                   \
        const std::string json(jsonstr);                \
        fmt::println("{}", json);                       \
                                                        \
        auto res = JsonImporter::import(batch, json);   \
        fmt::println("{}\n", res.error().fmtMessage()); \
        ASSERT_FALSE(res);                              \
    }

TEST_F(JsonImporterTest, importInvalidJson) {
    VecLibAccessor accessor = _db->getLibrary(_mylibID);

    BatchVectorCreate batch = accessor.prepareCreateBatch();

    INVALID_JSON("");
    INVALID_JSON("{");
    INVALID_JSON("[");
    INVALID_JSON("}");
    INVALID_JSON("]");
    INVALID_JSON("{\"id\": 123, \"vector\": [1.0, 2.0, 3.0, 4.0]}");
    INVALID_JSON("[]");
    INVALID_JSON("[ {} ]");
    INVALID_JSON("[ { \"hello\": \"world\" } ]");
    INVALID_JSON("[ { \"id\": 5.2 } ]");
    INVALID_JSON("[ { \"id\": \"hello\" } ]");
    INVALID_JSON("[ { \"id\": true } ]");
    INVALID_JSON("[ { \"id\": -5 } ]");
    INVALID_JSON("[ { \"id\": 5 } ]");
    INVALID_JSON("[ { \"id\": {} } ]");
    INVALID_JSON("[ { \"id\": [] } ]");
    INVALID_JSON("[ { \"id\": 5, \"hello\": [] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": 5 } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": {} } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [5] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [[]] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [{}] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [0.0, 5] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [0.0, []] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [0.0, {}] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [0.0, 1.0, 0.5, 1.2, 1.3, 1.2] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [0.0, 1.0, 0.5, 1.2, 1.3, 1.2, 1.1, 0.7, 0.8] } ]");
    INVALID_JSON("[ { \"id\": 5, \"vector\": [0.0, 1.0, 0.5, 1.2, 1.3, 1.2, 1.1, 0.7], {} } ]");
}

TEST_F(JsonImporterTest, ValidJson) {
    VecLibAccessor accessor = _db->getLibrary(_mylibID);

    BatchVectorCreate batch = accessor.prepareCreateBatch();

    {
        const std::string json = R"(
            [
                {
                    "id": 0,
                    "vector": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                },
                {
                    "id": 1,
                    "vector": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
                },
                {
                    "id": 2,
                    "vector": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
                }
            ]
        )";

        fmt::println("{}", json);

        auto res = JsonImporter::import(batch, json);
        ASSERT_TRUE(res);
        ASSERT_EQ(batch.count(), 3);
    }

    {
        const std::string json = R"(
            [
                {
                    "id": 10,
                    "vector": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                },
                {
                    "id": 11,
                    "vector": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
                },
                {
                    "id": 12,
                    "vector": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
                }
            ]
        )";

        fmt::println("{}", json);

        auto res = JsonImporter::import(batch, json);
        ASSERT_TRUE(res);
        ASSERT_EQ(batch.count(), 6);
    }

    {
        auto res = accessor.addEmbeddings(batch);
        ASSERT_TRUE(res);
    }

    batch.clear(8);

    {
        const std::string json = R"(
            [
                {
                    "id": 20,
                    "vector": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                },
                {
                    "id": 21,
                    "vector": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
                },
                {
                    "id": 22,
                    "vector": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
                }
            ]
        )";

        fmt::println("{}", json);

        auto res = JsonImporter::import(batch, json);
        ASSERT_TRUE(res);
        ASSERT_EQ(batch.count(), 3);
    }

    {
        auto res = accessor.addEmbeddings(batch);
        ASSERT_TRUE(res);
    }

    {
        constexpr std::array<float, 8> v {0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
        VectorSearchResult results;
        VectorSearchQuery query {8};
        query.setVector(v);
        query.setMaxResultCount(10);

        auto res = accessor.search(query, results);
        ASSERT_TRUE(res);
        results.finishSearch(1);
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 2; });
}
