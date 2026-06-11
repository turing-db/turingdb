#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "Graph.h"
#include "JobSystem.h"
#include "JsonlParser.h"
#include "LocalMemory.h"
#include "QueryCallbacks.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/Transaction.h"

using namespace db;

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("jsonl-import", "1.0", argparse::default_arguments::help);
    parser.add_description("Import a JSONL graph file into TuringDB");

    std::string inputFile;
    std::string query;

    parser.add_argument("file", "f")
        .metavar("graph.jsonl")
        .store_into(inputFile)
        .help("JSONL file to import");

    parser.add_argument("--query", "q")
        .metavar("QUERY")
        .store_into(query)
        .help("Cypher query to run after import");

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    std::ifstream file(inputFile);
    if (!file.is_open()) {
        fmt::println("Could not open file: {}", inputFile);
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

    constexpr std::string_view graphName = "test";
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

    if (!query.empty()) {
        LocalMemory mem;
        QueryCallbacks cbs;
        const auto dump = [](const Dataframe* d) { d->dump(std::cout); };
        cbs.setOnOutputData(dump);
        QueryState state(graphName, &mem, &db.getDefaultQueryConfig(), &cbs);
        const auto res = db.query(query, state);
        if (!res) {
            spdlog::error(res.getError());
            return EXIT_FAILURE;
        }
    }

    system.dumpGraph("test");

    return EXIT_SUCCESS;
}
