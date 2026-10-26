#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>

#include "ExtensionManager.h"
#include "FileUtils.h"
#include "GraphManager.h"
#include "JobSystem.h"
#include "Path.h"
#include "TuringConfig.h"
#include "TuringException.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A graph is a directory under the graphs directory and an extension a file under the
// extensions directories, both named after it. The managers that own those directories turn
// away a name that would reach outside them, whoever asks
class DirectoryNameValidationTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::createSyncedOnDisk(fs::Path {_outDir} / "turing");
        _graphs = std::make_unique<GraphManager>(&_env->getConfig());
        _jobSystem.init();
    }

    void writeGraphFile(std::string_view fileName) {
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};

        std::ofstream file(dataDir / std::string {fileName});
        ASSERT_TRUE(file.is_open());

        file << R"({"type":"node","id":"0","labels":["Person"],"properties":{}})" "\n";
    }

    bool existsBesideTheGraphsDirectory(std::string_view name) const {
        const fs::Path& turingDir = _env->getConfig().getTuringDir();
        return (turingDir / name).exists();
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<GraphManager> _graphs;
    JobSystem _jobSystem;
};

TEST_F(DirectoryNameValidationTest, createsAGraphWithAPlainName) {
    EXPECT_NE(_graphs->createGraph("plain_name_1"), nullptr);
}

TEST_F(DirectoryNameValidationTest, refusesToCreateAGraphOutsideTheGraphsDirectory) {
    EXPECT_THROW(_graphs->createGraph("../x"), TuringException);
    EXPECT_FALSE(existsBesideTheGraphsDirectory("x"));
}

TEST_F(DirectoryNameValidationTest, refusesToCreateAGraphInASubdirectory) {
    EXPECT_THROW(_graphs->createGraph("a/b"), TuringException);
}

TEST_F(DirectoryNameValidationTest, refusesToCreateAGraphWithAnEmptyName) {
    EXPECT_THROW(_graphs->createGraph(""), TuringException);
}

TEST_F(DirectoryNameValidationTest, refusesToLoadAGraphOutsideTheGraphsDirectory) {
    EXPECT_THROW(_graphs->loadGraph("../x"), TuringException);
}

TEST_F(DirectoryNameValidationTest, refusesToLoadAGraphInASubdirectory) {
    EXPECT_THROW(_graphs->loadGraph("a/b"), TuringException);
}

TEST_F(DirectoryNameValidationTest, refusesToImportAGraphOutsideTheGraphsDirectory) {
    writeGraphFile("people.jsonl");

    EXPECT_THROW(_graphs->importGraph("../x", fs::Path {"people.jsonl"}, &_jobSystem), TuringException);
    EXPECT_FALSE(existsBesideTheGraphsDirectory("x"));
}

TEST_F(DirectoryNameValidationTest, refusesToImportAGraphInASubdirectory) {
    writeGraphFile("people.jsonl");

    EXPECT_THROW(_graphs->importGraph("a/b", fs::Path {"people.jsonl"}, &_jobSystem), TuringException);
}

TEST_F(DirectoryNameValidationTest, refusesToInstallAnExtensionOutsideTheExtensionsDirectory) {
    ExtensionManager extensions;
    EXPECT_THROW(extensions.installExtension("../x"), TuringException);
}

TEST_F(DirectoryNameValidationTest, refusesToInstallAnExtensionInASubdirectory) {
    ExtensionManager extensions;
    EXPECT_THROW(extensions.installExtension("a/b"), TuringException);
}

TEST_F(DirectoryNameValidationTest, refusesToInstallAnExtensionWithAnEmptyName) {
    ExtensionManager extensions;
    EXPECT_THROW(extensions.installExtension(""), TuringException);
}
