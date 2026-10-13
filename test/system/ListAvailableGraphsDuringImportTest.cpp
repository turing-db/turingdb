#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "FileUtils.h"
#include "SystemAccessor.h"
#include "SystemManager.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class ListAvailableGraphsDuringImportTest : public TuringTest {
};

TEST_F(ListAvailableGraphsDuringImportTest, aListedGraphStaysListedWhileItFinishesLoading) {
    constexpr size_t GRAPH_COUNT = 1000;

    const std::unique_ptr<TuringTestEnv> env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
    SystemManager& systemManager = env->getSystemManager();

    {
        const FileUtils::Path dataDir {env->getConfig().getDataDir().get()};
        std::ofstream file(dataDir / "tiny.jsonl");
        ASSERT_TRUE(file.is_open());

        file << R"({"type":"node","id":"0","labels":["Person"],"properties":{"name":"a"}})" << '\n';
    }

    std::atomic<bool> importing {true};
    std::atomic<size_t> importedCount {0};

    std::thread importer([&systemManager, &importing, &importedCount] {
        for (size_t graph = 0; graph < GRAPH_COUNT; graph++) {
            SystemAccessor system = systemManager.accessShared();
            if (system.importGraph(fs::Path {"tiny.jsonl"}, "g" + std::to_string(graph), {}, {}, {})) {
                importedCount++;
            }
        }

        importing = false;
    });

    std::vector<std::string> previous;
    std::vector<std::string> current;
    size_t droppedListings = 0;

    while (importing) {
        current.clear();

        const SystemAccessor system = systemManager.accessShared();
        system.listAvailableGraphs(current);

        if (!std::ranges::includes(current, previous)) {
            droppedListings++;
        }

        std::swap(previous, current);
    }

    importer.join();

    EXPECT_EQ(importedCount, GRAPH_COUNT);
    EXPECT_EQ(droppedListings, 0);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
