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

    void expectError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _sessionGraph,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    void expectNoChangeOpen() {
        const SystemAccessor system = _env->getSystemManager().accessShared();
        EXPECT_FALSE(system.hasChanges());
    }

    const std::string _sessionGraph = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(FailedGraphLoadTest, leavesNoChangeOpenAfterAParquetImportFails) {
    writeDataFile("broken/nodes.parquet", "not parquet");
    writeDataFile("broken/edges.parquet", "not parquet");

    expectError("LOAD PARQUET 'broken' AS broken", "Parquet magic bytes not found");
    expectNoChangeOpen();
}

TEST_F(FailedGraphLoadTest, leavesNoChangeOpenAfterAJsonlImportFails) {
    writeDataFile("broken.jsonl", "not json\n");

    expectError("LOAD JSONL 'broken.jsonl' AS broken", "LOAD JSONL: failed to import graph 'broken'");
    expectNoChangeOpen();
}

TEST_F(FailedGraphLoadTest, leavesNoChangeOpenAfterAnImportOfAFileThatIsNotThere) {
    expectError("LOAD JSONL 'absent.jsonl' AS absent", "LOAD JSONL: failed to import graph 'absent'");
    expectNoChangeOpen();
}
