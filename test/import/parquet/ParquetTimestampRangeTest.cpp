#include "TuringTest.h"
#include "TuringTestEnv.h"

#include <string>
#include <string_view>

#include "FileUtils.h"
#include "Graph.h"
#include "JobSystem.h"
#include "Path.h"
#include "SystemManager.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// An instant leaves the graph as an ISO-8601 string with a four-digit year, which is also
// all DateTime::parse reads back, so a TIMESTAMP naming a year outside 0000-9999 would be
// imported as a value no client could re-parse. Parquet counts in milliseconds as readily
// as microseconds, so a value that clears the multiplication guard can still land far
// outside that range: it is malformed input, and the import says so.
class ParquetTimestampRangeTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem.reset();
        _env.reset();
    }

    void importFixture(SystemAccessor& system,
                       std::string_view graphName,
                       std::string_view nodeFixture) {
        const std::string importDirName {graphName};
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / importDirName;
        FileUtils::createDirectory(importDir);

        const FileUtils::Path testDataDir {PARQUET_TEST_DATA_DIR};
        FileUtils::copy(testDataDir / std::string {nodeFixture}, importDir / "nodes.parquet");
        FileUtils::copy(testDataDir / "minimal_edges.parquet", importDir / "edges.parquet");

        system.importGraph(fs::Path {importDirName}, graphName);
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(ParquetTimestampRangeTest, RejectsATimestampOutsideTheRenderableYears) {
    SystemAccessor system = _env->getSystemManager().accessUnique();

    EXPECT_THROW(importFixture(system, "outofrange", "datetime_out_of_range_nodes.parquet"),
                 TuringException);
}

// The instants the importer's own fixture carries are all well inside the range, so the
// bound turns away nothing an ordinary export holds
TEST_F(ParquetTimestampRangeTest, ImportsTheTimestampsOfAnOrdinaryExport) {
    SystemAccessor system = _env->getSystemManager().accessUnique();

    EXPECT_NO_THROW(importFixture(system, "inrange", "datetime_property_nodes.parquet"));
}
