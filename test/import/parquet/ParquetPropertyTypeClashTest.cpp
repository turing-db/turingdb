#include "TuringTest.h"
#include "TuringTestEnv.h"

#include <string>
#include <string_view>

#include "FileUtils.h"
#include "JobSystem.h"
#include "Path.h"
#include "SystemManager.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// A split export shares one property-type map across its node and edge files, and a name
// already registered keeps the type it was registered with. So a file discovering that name
// at another type would write its values into a container the metadata calls something
// else. A DateTime and an Int64 are both eight bytes, which makes that pair read back as
// plausible instants rather than failing, so the import turns the clash away instead.
class ParquetPropertyTypeClashTest : public TuringTest {
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

    void importSplit(SystemAccessor& system,
                     std::string_view graphName,
                     std::string_view nodeFixture,
                     std::string_view edgeFixture) {
        const std::string importDirName {graphName};
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / importDirName;
        FileUtils::createDirectory(importDir);

        const FileUtils::Path testDataDir {PARQUET_TEST_DATA_DIR};
        FileUtils::copy(testDataDir / std::string {nodeFixture}, importDir / "nodes.parquet");
        FileUtils::copy(testDataDir / std::string {edgeFixture}, importDir / "edges.parquet");

        system.importGraph(fs::Path {importDirName}, graphName);
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(ParquetPropertyTypeClashTest, RejectsOneNameDiscoveredAtTwoTypes) {
    SystemAccessor system = _env->getSystemManager().accessUnique();

    EXPECT_THROW(importSplit(system,
                             "clash",
                             "datetime_name_clash_nodes.parquet",
                             "datetime_name_clash_edges.parquet"),
                 TuringException);
}

// Node and edge files naming different properties share nothing, so the check turns away
// nothing an ordinary export holds
TEST_F(ParquetPropertyTypeClashTest, ImportsAnExportWhoseNamesDoNotClash) {
    SystemAccessor system = _env->getSystemManager().accessUnique();

    EXPECT_NO_THROW(importSplit(system,
                                "clean",
                                "datetime_property_nodes.parquet",
                                "datetime_property_edges.parquet"));
}
