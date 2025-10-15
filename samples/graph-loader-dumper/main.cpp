#include "Graph.h"
#include "writers/DataPartBuilder.h"
#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "comparators/GraphComparator.h"
#include "LogUtils.h"
#include "TuringTime.h"
#include "SimpleGraph.h"
#include "JobSystem.h"
#include "versioning/CommitBuilder.h"
#include "FileUtils.h"
#include "Neo4jImporter.h"
#include "Profiler.h"
#include "Neo4j/Neo4JParserConfig.h"
#include "Neo4jImporter.h"

using namespace db;

namespace {

bool testGraph(const Graph& graph, const fs::Path& path) {
    Profiler::clear();
    fmt::print("- Dumping graph to: {}\n", path.c_str());

    {
        const auto t0 = Clock::now();
        if (auto res = GraphDumper::dump(graph, path); !res) {
            fmt::print("{}\n", res.error().fmtMessage());
            return false;
        }
        const auto t1 = Clock::now();
        logt::ElapsedTime(duration<Seconds>(t0, t1), "s");

        std::string profilerOutput;
        Profiler::dumpAndClear(profilerOutput);
        if (!profilerOutput.empty()) {
            fmt::print("{}\n", profilerOutput);
        }
    }

    fmt::print("- Loading graph from: {}\n", path.c_str());

    const auto t0 = Clock::now();
    auto loadedGraph = Graph::create();
    auto loadedGraphRes = GraphLoader::load(loadedGraph.get(), path);

    std::string profilerOutput;
    Profiler::dumpAndClear(profilerOutput);
    if (!profilerOutput.empty()) {
        fmt::print("{}\n", profilerOutput);
    }

    if (!loadedGraphRes) {
        fmt::print("{}\n", loadedGraphRes.error().fmtMessage());
        return false;
    }
    const auto t1 = Clock::now();
    logt::ElapsedTime(duration<Seconds>(t0, t1), "s");


    if (!GraphComparator::same(graph, *loadedGraph)) {
        fmt::print("Loaded graph is not the same as the one dumped\n");
        return false;
    }

    fmt::print("Loaded graph is the same as the one dumped\n");
    return true;
}

}

int main() {
    {
        // Load & dump simple graph
        const fs::Path path {SAMPLE_DIR "/simple-graph"};

        auto graph = Graph::create();
        SimpleGraph::createSimpleGraph(graph.get());

        if (path.exists()) {
            // Removing existing dir
            if (auto res = path.rm(); !res) {
                fmt::print("{}\n", res.error().fmtMessage());
                return 1;
            }
        }

        if (!testGraph(*graph, path)) {
            return 1;
        }
    }

    {
        // Dump pole
        const fs::Path path {SAMPLE_DIR "/pole"};

        auto jobSystem = JobSystem::create();

        auto graph = Graph::create();
        const std::string turingHome = std::getenv("TURING_HOME");
        const fs::Path jsonDir = fs::Path {turingHome} / "neo4j" / "pole-db";

        Neo4jImporter neo4jImporter;
        if (!neo4jImporter.importJsonDir(*jobSystem,
                                         graph.get(),
                                         db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                         db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                         {
                                             ._jsonDir = FileUtils::Path {jsonDir.get()},
                                         })) {
            fmt::print("Could not load Pole\n");
            return 1;
        }

        if (path.exists()) {
            // Removing existing dir
            if (auto res = path.rm(); !res) {
                fmt::print("{}\n", res.error().fmtMessage());
                return 1;
            }
        }

        if (!testGraph(*graph, path)) {
            return 1;
        }
    }

        /*
    {
        // Dump reactome
        auto t0 = Clock::now();
        const fs::Path path {SAMPLE_DIR "/reactome"};

        auto jobSystem = JobSystem::create();

        auto graph = Graph::create();
        const std::string turingHome = std::getenv("TURING_HOME");
        const fs::Path jsonDir = fs::Path {turingHome} / "neo4j" / "reactome";

        Neo4jImporter neo4jImporter;
        if (!neo4jImporter.importJsonDir(*jobSystem,
                                          graph.get(),
                                          db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                          db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                          {
                                              ._jsonDir = FileUtils::Path {jsonDir.get()},
                                          })) {
            fmt::print("Could not load Reactome\n");
            return 1;
        }

        const auto t1 = Clock::now();
        logt::ElapsedTime(duration<Seconds>(t0, t1), "s");

        if (path.exists()) {
            // Removing existing dir
            if (auto res = path.rm(); !res) {
                fmt::print("{}\n", res.error().fmtMessage());
                return 1;
            }
        }

        if (!testGraph(*graph, path)) {
            return 1;
        }
    }
    */

    // {
    //     // Dump ckg
    //     const fs::Path path {SAMPLE_DIR "/ckg"};

    //     JobSystem jobSystem {1};
    //     jobSystem.initialize();

    //     auto graph = Graph::create();
    //     const std::string turingHome = std::getenv("HOME");
    //     const fs::Path jsonDir = fs::Path {turingHome} / "graphs_v2" / "ckg";

    //     if (!Neo4jImporter::importJsonDir(jobSystem,
    //                                  graph.get(),
    //                                  db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
    //                                  db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
    //                                  {
    //                                      ._jsonDir = FileUtils::Path {jsonDir.get()},
    //                                  })) {
    //         fmt::print("Could not load CKG\n");
    //         return 1;
    //     }

    //     if (path.exists()) {
    //         // Removing existing dir
    //         if (auto res = path.rm(); !res) {
    //             fmt::print("{}\n", res.error().fmtMessage());
    //             return 1;
    //         }
    //     }

    //    if (!testGraph(*graph, path)) {
    //        return 1;
    //    }
    // }

    return 0;
}
