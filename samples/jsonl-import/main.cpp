#include <fstream>
#include <memory>

#include "Graph.h"
#include "JobSystem.h"
#include "JsonlParser.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/Transaction.h"

using namespace db;

int main(int argc, const char** argv) {
    if (argc < 2) {
        fmt::println("Usage: {} <jsonl-file>", argv[0]);
        return EXIT_FAILURE;
    }

    std::ifstream file(argv[1]);
    if (!file.is_open()) {
        fmt::println("Could not open file: {}", argv[1]);
        return EXIT_FAILURE;
    }

    auto js = std::make_unique<JobSystem>();
    js->init();

    const fs::Path turingDir(SAMPLE_DIR "/.turing");
    if (turingDir.exists()) {
        fmt::println("Turing directory already exists: {}", turingDir.get());
        fmt::println("Removing it...");

        if (const auto res = turingDir.rm(); !res) {
            fmt::println("Could not remove turing directory: {}", res.error().fmtMessage());
            return EXIT_FAILURE;
        }
    }

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    TuringDB db(&config);

    SystemManager& sysMan = db.getSystemManager();
    SystemAccessor system = sysMan.accessUnique();
    Graph* graph = system.createGraph("test");

    std::unique_ptr<Change> change = graph->newChange();
    ChangeAccessor accessor(change->access());

    {
        const auto res = JsonlParser::parse(accessor, file);
        if (!res) {
            fmt::println("{}", res.error().fmtMessage());
            return EXIT_FAILURE;
        }
    }

    {
        const auto res = accessor.submit(*js);
        if (!res) {
            fmt::println("{}", res.error().fmtMessage());
            return EXIT_FAILURE;
        }
    }

    fmt::println("Submitted change");
    auto tx = graph->openTransaction();
    const GraphReader reader = tx.readGraph();
    fmt::println("Graph has {} nodes and {} edges", reader.getNodeCount(), reader.getEdgeCount());

    system.dumpGraph("test");

    return EXIT_SUCCESS;
}
