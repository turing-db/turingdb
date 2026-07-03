#include "TuringTest.h"

#include <string_view>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "QueryStatus.h"
#include "SystemManager.h"

#include "FileUtils.h"
#include "Path.h"

#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class LoadParquetQueryTest : public TuringTest {
public:
    void initialize() override {
        const auto turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(turingDir);
        _db = &_env->getDB();

        // Place a split-Parquet export (nodes.parquet + edges.parquet) inside the
        // data directory so LOAD PARQUET can resolve it by relative path.
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / "typed";
        FileUtils::createDirectory(importDir);

        const FileUtils::Path testDataDir {PARQUET_TEST_DATA_DIR};
        FileUtils::copy(testDataDir / "nodes.parquet", importDir / "nodes.parquet");
        FileUtils::copy(testDataDir / "edges.parquet", importDir / "edges.parquet");
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;

    QueryStatus query(std::string_view q, std::string_view graphName = "default") {
        QueryCallbacks callbacks;
        const QueryState state(std::string(graphName), &_env->getMem(), &_queryConfig, &callbacks);
        return _db->query(q, state);
    }
};

TEST_F(LoadParquetQueryTest, loadParquetSucceeds) {
    EXPECT_TRUE(query("LOAD PARQUET 'typed' AS typed"));
}

TEST_F(LoadParquetQueryTest, canQueryLoadedGraph) {
    ASSERT_TRUE(query("LOAD PARQUET 'typed' AS typed"));
    EXPECT_TRUE(query("MATCH (n) RETURN n", "typed"));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
