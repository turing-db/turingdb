#include <stdlib.h>

#include <chrono>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "dump/GraphLoader.h"
#include "dump/parquet/GraphParquetDumper.h"
#include "dump/parquet/GraphParquetLoader.h"
#include "comparators/GraphComparator.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/VersionController.h"
#include "Graph.h"
#include "Path.h"

#include "TuringException.h"
#include "ToolInit.h"

using namespace db;

namespace {

double secondsSince(const std::chrono::steady_clock::time_point& start) {
    const auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

}

// Loads a graph through the old binary GraphLoader, round-trips it through the new Parquet
// GraphParquetDumper / GraphParquetLoader, and asserts the binary-loaded graph and the
// Parquet-round-tripped graph are structurally identical via GraphComparator::same. Built to
// validate the Parquet serializer against the full Reactome graph.
int main(int argc, const char** argv) {
    ToolInit toolInit("parquet-roundtrip");

    auto& argParser = toolInit.getArgParser();

    std::string graphDirArg;
    argParser.add_argument("-graph-dir")
             .metavar("path")
             .store_into(graphDirArg)
             .help("Binary graph dump directory to load (the old binary format)");

    std::string outDirArg;
    argParser.add_argument("-out-dir")
             .metavar("path")
             .store_into(outDirArg)
             .help("Directory to write the Parquet dump into");

    toolInit.init(argc, argv);

    if (graphDirArg.empty()) {
        spdlog::error("Provide -graph-dir <binary dump directory>");
        return EXIT_FAILURE;
    }

    const fs::Path binaryDir {graphDirArg};

    fs::Path parquetDir = outDirArg.empty()
                              ? fs::Path {toolInit.getOutputsDir()} / "parquet-roundtrip" / "dump"
                              : fs::Path {outDirArg};

    if (parquetDir.exists()) {
        parquetDir.rm();
    }

    // 1. Load through the old binary loader.
    auto binaryGraph = Graph::create();
    {
        const auto start = std::chrono::steady_clock::now();

        const auto res = GraphLoader::load(binaryGraph.get(), binaryDir);
        if (!res) {
            spdlog::error("Binary load failed: {}", res.error().fmtMessage());
            return EXIT_FAILURE;
        }

        spdlog::info("Binary-loaded '{}' in {:.2f}s", binaryGraph->getName(), secondsSince(start));
    }

    {
        const VersionController& controller = binaryGraph->getVersionController();
        spdlog::info("Commit count: {}", controller.getNumCommits());

        for (const Commit* commit = controller.getCommitSafe(CommitHash::head());
             commit != nullptr;
             commit = commit->getPreviousCommit()) {
            spdlog::info("  commit {}: hasData={} nodes={} edges={} dataparts={}",
                         commit->hash().get(),
                         commit->hasData(),
                         commit->getNumNodes(),
                         commit->getNumEdges(),
                         commit->getNumDataParts());
        }
    }

    // 2. Dump it through the new Parquet dumper.
    {
        const auto start = std::chrono::steady_clock::now();

        try {
            GraphParquetDumper::dump(*binaryGraph, parquetDir);
        } catch (const std::exception& e) {
            spdlog::error("Parquet dump failed: {}", e.what());
            return EXIT_FAILURE;
        }

        spdlog::info("Parquet-dumped to {} in {:.2f}s", parquetDir.c_str(), secondsSince(start));
    }

    // 3. Load the Parquet dump back.
    auto parquetGraph = Graph::create();
    {
        const auto start = std::chrono::steady_clock::now();

        try {
            GraphParquetLoader::load(parquetGraph.get(), parquetDir);
        } catch (const std::exception& e) {
            spdlog::error("Parquet load failed: {}", e.what());
            return EXIT_FAILURE;
        }

        spdlog::info("Parquet-loaded in {:.2f}s", secondsSince(start));
    }

    // 4. Compare the binary-loaded graph with the Parquet round-tripped graph.
    {
        const auto start = std::chrono::steady_clock::now();

        const bool same = GraphComparator::same(*binaryGraph, *parquetGraph);

        spdlog::info("GraphComparator::same = {} (compared in {:.2f}s)", same, secondsSince(start));

        if (!same) {
            spdlog::error("MISMATCH: the Parquet round-trip does not match the binary-loaded graph");
            return EXIT_FAILURE;
        }
    }

    spdlog::info("PASS: full Parquet round-trip matches the old binary loader/dumper");
    return EXIT_SUCCESS;
}
