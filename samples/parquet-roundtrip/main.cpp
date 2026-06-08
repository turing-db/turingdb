#include <stdint.h>
#include <stdlib.h>

#include <chrono>
#include <span>
#include <vector>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "dump/parquet/GraphParquetDumper.h"
#include "dump/parquet/GraphParquetLoader.h"
#include "comparators/GraphComparator.h"
#include "datapart/DataPart.h"
#include "datapart/DataPartSpan.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/VersionController.h"
#include "writers/GraphWriter.h"
#include "Graph.h"
#include "Path.h"

#include "JobSystem.h"
#include "TuringException.h"
#include "ToolInit.h"

using namespace db;

namespace {

double secondsSince(const std::chrono::steady_clock::time_point& start) {
    const auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

// splitmix64: fast deterministic generator, two uniform floats in [0, 1) per draw.
uint64_t nextRandomValue(uint64_t& randomState) {
    randomState += 0x9E3779B97F4A7C15ull;

    uint64_t mixed = randomState;
    mixed = (mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9ull;
    mixed = (mixed ^ (mixed >> 27)) * 0x94D049BB133111EBull;
    return mixed ^ (mixed >> 31);
}

void fillRandomEmbedding(std::vector<float>& embedding, uint64_t& randomState) {
    constexpr float scale = 1.0f / 16777216.0f;  // 2^-24

    for (size_t index = 0; index < embedding.size(); index += 2) {
        const uint64_t bits = nextRandomValue(randomState);

        embedding[index] = static_cast<float>(bits >> 40) * scale;
        if (index + 1 < embedding.size()) {
            embedding[index + 1] = static_cast<float>((bits >> 16) & 0xFFFFFF) * scale;
        }
    }
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

    bool dumpOnlyArg {false};
    argParser.add_argument("-dump-only")
             .store_into(dumpOnlyArg)
             .help("Stop after the Parquet dump (skip the load-back and comparison)");

    bool loadOnlyArg {false};
    argParser.add_argument("-load-only")
             .store_into(loadOnlyArg)
             .help("Stop after the binary load (skip the dump, load-back and comparison)");

    std::string dumpBinaryArg;
    argParser.add_argument("-dump-binary")
             .metavar("path")
             .store_into(dumpBinaryArg)
             .help("After the binary load, dump through the binary GraphDumper to this directory and exit");

    std::string loadParquetArg;
    argParser.add_argument("-load-parquet")
             .metavar("path")
             .store_into(loadParquetArg)
             .help("Load an existing Parquet dump, report the load time, and exit");

    int embeddingDimensionArg {0};
    argParser.add_argument("-add-random-embeddings")
             .metavar("dimension")
             .store_into(embeddingDimensionArg)
             .help("After the binary load, add a random embedding property of this dimension to every node");

    toolInit.init(argc, argv);

    if (!loadParquetArg.empty()) {
        const fs::Path parquetDumpDir {loadParquetArg};

        auto parquetGraph = Graph::create();
        const auto start = std::chrono::steady_clock::now();

        try {
            GraphParquetLoader::load(parquetGraph.get(), parquetDumpDir);
        } catch (const std::exception& e) {
            spdlog::error("Parquet load failed: {}", e.what());
            return EXIT_FAILURE;
        }

        spdlog::info("Parquet-loaded '{}' from {} in {:.2f}s",
                     parquetGraph->getName(),
                     parquetDumpDir.c_str(),
                     secondsSince(start));
        return EXIT_SUCCESS;
    }

    if (graphDirArg.empty()) {
        spdlog::error("Provide -graph-dir <binary dump directory>");
        return EXIT_FAILURE;
    }

    const fs::Path binaryDir {graphDirArg};

    const fs::Path parquetDir = outDirArg.empty()
                                    ? fs::Path {toolInit.getOutputsDir()} / "parquet-roundtrip" / "dump"
                                    : fs::Path {outDirArg};

    if (!loadOnlyArg && parquetDir.exists()) {
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

    if (embeddingDimensionArg > 0) {
        const size_t dimension = static_cast<size_t>(embeddingDimensionArg);
        const auto start = std::chrono::steady_clock::now();

        JobSystem jobSystem;
        jobSystem.init();

        {
            GraphWriter writer(binaryGraph.get(), &jobSystem);

            const PropertyType embeddingPropertyType = writer.addPropertyType("embedding", types::Embedding::_valueType);

            std::vector<float> embedding(dimension);
            uint64_t randomState {42};
            size_t totalNodes = 0;

            const Commit* head = binaryGraph->getVersionController().getCommitSafe(CommitHash::head());

            for (const auto& part : head->data().allDataparts()) {
                const NodeID firstNodeID = part->getFirstNodeID();
                const size_t nodeCount = part->getNodeContainerSize();

                for (size_t index = 0; index < nodeCount; ++index) {
                    fillRandomEmbedding(embedding, randomState);
                    writer.addNodeProperty<types::Embedding>(NodeID {firstNodeID + index},
                                                             embeddingPropertyType,
                                                             std::span<const float>(embedding));
                }

                totalNodes += nodeCount;
            }

            if (!writer.submit()) {
                spdlog::error("Committing the random embeddings failed");
                return EXIT_FAILURE;
            }

            spdlog::info("Added random {}-dim embeddings to {} nodes in {:.2f}s",
                         dimension,
                         totalNodes,
                         secondsSince(start));
        }
    }

    if (loadOnlyArg) {
        spdlog::info("-load-only: skipping the dump, load-back and comparison");
        return EXIT_SUCCESS;
    }

    if (!dumpBinaryArg.empty()) {
        const fs::Path binaryOutDir {dumpBinaryArg};
        if (binaryOutDir.exists()) {
            binaryOutDir.rm();
        }

        const auto start = std::chrono::steady_clock::now();

        const auto res = GraphDumper::dump(*binaryGraph, binaryOutDir);
        if (!res) {
            spdlog::error("Binary dump failed: {}", res.error().fmtMessage());
            return EXIT_FAILURE;
        }

        spdlog::info("Binary-dumped to {} in {:.2f}s", binaryOutDir.c_str(), secondsSince(start));
        return EXIT_SUCCESS;
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

    if (dumpOnlyArg) {
        spdlog::info("-dump-only: skipping the load-back and comparison");
        return EXIT_SUCCESS;
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
