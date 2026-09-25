#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "FileUtils.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A load that fails after opening the change it imports into drops that change with the
// graph it was building. MERGE_DATAPARTS refuses to run while any change is open, so it is
// the query that sees a change left behind.
class FailedGraphLoadTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_sessionGraph);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void writeDataFile(const FileUtils::Path& relativePath, const std::string& content) {
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path path = dataDir / relativePath;

        FileUtils::createDirectory(path.parent_path());
        ASSERT_TRUE(FileUtils::writeFile(path, content)) << path;
    }

    void runQuery(std::string_view query, QueryStatus& status) {
        StringRowSink sink;
        _interpreter->execute(status,
                              query,
                              _sessionGraph,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }

    void expectError(std::string_view query, std::string_view reason) {
        QueryStatus status;
        runQuery(query, status);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    void expectSuccess(std::string_view query) {
        QueryStatus status;
        runQuery(query, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    const std::string _sessionGraph = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(FailedGraphLoadTest, mergesDataPartsAfterAParquetImportFails) {
    writeDataFile("broken/nodes.parquet", "not parquet");
    writeDataFile("broken/edges.parquet", "not parquet");

    expectError("LOAD PARQUET 'broken' AS broken", "Parquet magic bytes not found");
    expectSuccess("MERGE_DATAPARTS");
}

TEST_F(FailedGraphLoadTest, mergesDataPartsAfterAJsonlImportFails) {
    writeDataFile("broken.jsonl", "not json\n");

    expectError("LOAD JSONL 'broken.jsonl' AS broken", "LOAD JSONL: failed to import graph 'broken'");
    expectSuccess("MERGE_DATAPARTS");
}

TEST_F(FailedGraphLoadTest, mergesDataPartsAfterAnImportOfAFileThatIsNotThere) {
    expectError("LOAD JSONL 'absent.jsonl' AS absent", "LOAD JSONL: failed to import graph 'absent'");
    expectSuccess("MERGE_DATAPARTS");
}
