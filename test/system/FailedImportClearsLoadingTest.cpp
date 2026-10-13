#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "FileUtils.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringException.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class FailedImportClearsLoadingTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
    }

    void writeFixture(std::string_view fileName, std::string_view contents) {
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};

        std::ofstream file(dataDir / std::string {fileName});
        ASSERT_TRUE(file.is_open());

        file << contents;
    }

    void expectImportFails(std::string_view fileName) {
        SystemAccessor system = _env->getSystemManager().accessShared();

        Graph* graph = nullptr;
        try {
            graph = system.importGraph(fs::Path {std::string {fileName}}, "g", {}, {}, {});
        } catch (const TuringException&) {
        }

        EXPECT_EQ(graph, nullptr) << fileName;
        EXPECT_FALSE(system.isGraphLoading("g")) << fileName;

        std::vector<std::string> names;
        system.listAvailableGraphs(names);
        EXPECT_EQ(names, std::vector<std::string> {"default"}) << fileName;
    }

    std::unique_ptr<TuringTestEnv> _env;
};

TEST_F(FailedImportClearsLoadingTest, aMalformedFileLeavesTheNameFree) {
    writeFixture("unlabelled.jsonl", R"({"type":"node","id":"0","labels":[],"properties":{}})" "\n");
    writeFixture("nolabels.jsonl", R"({"type":"node","id":"0","properties":{}})" "\n");
    writeFixture("badid.jsonl", R"({"type":"node","id":"x","labels":["P"],"properties":{}})" "\n");
    writeFixture("untyped.jsonl",
                 R"({"type":"node","id":"0","labels":["P"],"properties":{}})" "\n"
                 R"({"type":"relationship","id":"0","label":null,"properties":{},"start":0,"end":0})" "\n");
    writeFixture("good.jsonl", R"({"type":"node","id":"0","labels":["P"],"properties":{}})" "\n");

    for (const std::string_view fileName : {"unlabelled.jsonl", "nolabels.jsonl", "badid.jsonl", "untyped.jsonl"}) {
        expectImportFails(fileName);
    }

    SystemAccessor system = _env->getSystemManager().accessShared();
    EXPECT_NE(system.importGraph(fs::Path {"good.jsonl"}, "g", {}, {}, {}), nullptr);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
